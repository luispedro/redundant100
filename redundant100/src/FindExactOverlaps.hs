#!/usr/bin/env stack
-- stack --resolver lts-16.18 script
{-# LANGUAGE LambdaCase, OverloadedStrings, BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

import qualified Language.C.Inline as C
import qualified Language.C.Inline.Unsafe as CU


import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Search as BSearch
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as C
import qualified Conduit as C
import           Data.Conduit ((.|))
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM
import qualified Data.IntMap.Strict as IM
import qualified Data.IntSet as IS
import           Safe (headDef, atDef)
import           System.IO.SafeWrite (withOutputFile)
import           Data.Strict.Tuple (Pair(..))
import           Control.Concurrent (setNumCapabilities)
import           System.IO.Unsafe (unsafeDupablePerformIO)
import           Control.Monad.ST.Unsafe (unsafeIOToST)
import           System.Environment
import           Control.Monad.Extra (whenJust)
import           Control.Applicative ((<|>))
import Control.Monad
import System.Console.GetOpt
import Data.Maybe
import Data.List
import Control.Monad.IO.Class
import qualified Data.HashTable.IO as HT
import Data.Int
import Data.Word

import Data.BioConduit
import Data.Conduit.Algorithms.Utils
import Data.Conduit.Algorithms.Async


C.context (C.baseCtx <> C.bsCtx <> C.vecCtx)
C.include "<stdint.h>"

rollall :: Int -> B.ByteString -> VS.Vector Int
rollall n bs = VS.unsafeCast . unsafeDupablePerformIO $ do
        let n' :: C.CInt
            n' = toEnum n
        res <- VSM.new (B.length bs - n + 1)
        [CU.block| void { rollhash($bs-ptr:bs, $bs-len:bs, $(int n'), $vec-ptr:(int64_t* res)); } |]
        VS.unsafeFreeze res


type FastaIOHash = HT.CuckooHashTable Int [Fasta]
type FastaMap = IM.IntMap [Fasta]
data SizedHash = SizedHash !Int !FastaMap

faContainedIn :: Fasta -> Fasta -> Bool
faContainedIn (Fasta _ da) (Fasta _ db) = not . null $ BSearch.indices da db


printEvery10k :: (MonadIO m) => C.Conduit a m a
printEvery10k = printEvery10k' (1 :: Int) (10000 :: Int)
    where
        printEvery10k' n 0 = do
            liftIO $ putStrLn ("Read "++show (n *10) ++"k reads")
            printEvery10k' (n+1) 10000
        printEvery10k' n r = C.await >>= \case
            Nothing -> return ()
            Just v -> do
                C.yield v
                printEvery10k' n (r - 1)

annotate :: Int -> Fasta -> (Fasta, (VS.Vector Int))
annotate hashSize fa@(Fasta _ fad) = (fa, rollall hashSize fad)

buildHash :: (MonadIO m, C.PrimMonad m, C.MonadUnliftIO m) => Int -> C.Sink Fasta m SizedHash
buildHash nthreads = CC.peek >>= \case
        Nothing -> error "Empty stream"
        Just first -> do
            let hashSize = faseqLength first
            SizedHash hashSize <$>
                    (C.conduitVector 4096
                    .| asyncMapC (2 * nthreads) (V.map (annotate hashSize))
                    .| CL.fold insertmany IM.empty)
    where
        insertmany imap = V.foldl addHash imap
        addHash imap (faseq, hashes) = IM.alter (Just . (faseq:) . (fromMaybe [])) curk imap
            where
                hashes' = VS.toList hashes
                curk = headDef (head hashes') (filter (flip IM.notMember imap) hashes')

findOverlapsSingle :: (MonadIO m, C.PrimMonad m) => Int -> C.ConduitT Fasta B.ByteString m ()
findOverlapsSingle nthreads = do
    first <- CC.peek
    h <- liftIO HT.new
    whenJust first $ \faseq ->
        CC.conduitVector 4096
            .| asyncMapC nthreads (V.map (annotate (faseqLength faseq)))
            .| CC.concat
            .| findOverlapsSingle' h
  where
    findOverlapsSingle' :: MonadIO m => FastaIOHash -> C.Conduit (Fasta, VS.Vector Int) m B.ByteString
    findOverlapsSingle' imap = awaitJust $ \(faseq@(Fasta sid _), hashes) -> do
        let lookup1 :: Pair [Fasta] (Maybe Int) -> Int -> IO (Pair [Fasta] (Maybe Int))
            lookup1 (ccovered :!: mcurk) h = do
                val <- HT.lookup imap h
                return $! case val of
                    Nothing -> (ccovered :!: mcurk <|> Just h)
                    Just prevs -> (filter (`faContainedIn` faseq) prevs ++ ccovered :!: mcurk)
        (covered :!: minsertpos) <- liftIO $ VS.foldM lookup1 ([] :!: Nothing) hashes
        -- If we failed to find an empty slot for the sequence, just use first one
        let insertpos = fromMaybe (VS.head hashes) minsertpos

        C.yield $ B.concat [B.concat [seqheader c, "\tC\t", sid, "\n"] | c <- covered]
        liftIO $ HT.mutate imap insertpos (\curv -> (Just . (faseq:) . fromMaybe [] $ curv, ()))
        findOverlapsSingle' imap

findOverlapsAcross :: (MonadIO m, C.PrimMonad m) => Int -> SizedHash -> C.Conduit Fasta m B.ByteString
findOverlapsAcross n (SizedHash hashSize imap) = CC.conduitVector 4096 .| asyncMapC (2 * n) findOverlapsAcross'
    where
        findOverlapsAcross' :: V.Vector Fasta -> B.ByteString
        findOverlapsAcross' = B.concat . concatMap findOverlapsAcross'1
        findOverlapsAcross'1 :: Fasta -> [B.ByteString]
        findOverlapsAcross'1 faseq@(Fasta sid _) = concat [[c, "\tC\t", sid, "\n"] | c <- covered]
            where
                hashes :: IS.IntSet
                hashes = IS.fromList . VS.toList $ rollall hashSize (seqdata faseq)

                candidates :: [Fasta]
                candidates = concat . IM.elems $ IM.restrictKeys imap hashes

                covered :: [B.ByteString]
                covered = mapMaybe (`matches` faseq) candidates
                matches (Fasta h s) (Fasta _ seq')
                    | B.isInfixOf s seq' = Just h
                    | otherwise = Nothing

data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | FilesFromFile FilePath
                | NJobs Int
                | Verbose
                | Mode2Flag
                deriving (Eq, Show)

options :: [OptDescr CmdFlags]
options =
    [ Option ['v'] ["verbose"] (NoArg Verbose)  "verbose mode"
    , Option ['2'] [] (NoArg Mode2Flag)         "2 file mode"
    , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file to check"
    , Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
    , Option ['e'] ["extra-list"] (ReqArg FilesFromFile "FILE") "Input files to check against"
    , Option ['j'] ["threads", "jobs"] (ReqArg (NJobs . read) "INT") "Nr threads"
    ]

data Mode = ModeSingle | ModeAcross
    deriving (Eq,Show)

data CmdArgs = CmdArgs
                    { ifile :: FilePath
                    , ofile :: FilePath
                    , mode :: Mode
                    , verbose :: Bool
                    , nJobs :: Int
                    , extraFilesFilesFrom :: FilePath
                    } deriving (Eq, Show)

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs iarg oarg ModeSingle False 8 []) flags
    where
        (flags, args, _extraOpts) = getOpt Permute options argv
        iarg = atDef "" args 0
        oarg = atDef "" args 1
        p c Verbose = c { verbose = True }
        p c (OutputFile o) = c { ofile = o }
        p c (InputFile i) = c { ifile = i }
        p c (FilesFromFile i) = c { extraFilesFilesFrom = i }
        p c (NJobs n) = c { nJobs = n }
        p c Mode2Flag = c { mode = ModeAcross }

main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    print opts
    let nthreads = nJobs opts
    setNumCapabilities nthreads
    case mode opts of
        ModeSingle ->
            C.runResourceT $ withOutputFile (ofile opts) $ \hout ->
                withPossiblyCompressedFile (ifile opts) $ \cinput ->
                    C.runConduit $
                        cinput
                        .| faConduit
                        .| findOverlapsSingle nthreads
                        .| C.sinkHandle hout
        ModeAcross -> do
            h <- C.runResourceT $
                withPossiblyCompressedFile (ifile opts) $ \cin ->
                    C.runConduit $
                        cin
                        .| faConduit
                        .| buildHash nthreads
            putStrLn "Built initial hash"
            extraFiles <-
                C.runConduitRes $
                    C.sourceFile (extraFilesFilesFrom opts)
                    .| C.lines
                    .| CL.map B8.unpack
                    .| CL.consume
            withOutputFile (ofile opts) $ \hout ->
                forM_ extraFiles $ \fafile -> do
                    putStrLn ("Handling file "++fafile)
                    C.runResourceT $ withPossiblyCompressedFile fafile $ \cinput ->
                        C.runConduit $
                            cinput
                            .| faConduit
                            .| findOverlapsAcross nthreads h
                            .| C.sinkHandle hout
            putStrLn "Done."
