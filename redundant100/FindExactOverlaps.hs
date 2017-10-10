#!/usr/bin/env stack
-- stack --resolver lts-8.13 script
{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Search as BSearch
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as C
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.IntMap.Strict as IM
import           Safe (headDef, atDef)
import           System.IO.SafeWrite (withOutputFile)
import           Data.Strict.Tuple (Pair(..))
import           Control.Concurrent (setNumCapabilities)
import System.Environment
import           Control.Monad.Extra (whenJust)
import           Control.Applicative ((<|>))
import Control.Monad
import System.Console.GetOpt
import Data.Maybe
import Data.Bits
import Data.List
import Control.Monad.IO.Class
import Control.Monad.ST
import Control.Monad.Base
import Data.Word
import Data.Hash
import qualified Data.HashTable.IO as HT

import Data.BioConduit
import Data.Conduit.Algorithms.Utils
import Data.Conduit.Algorithms.Async

-- This is the same as Data.Hash.combine, but we cannot use it because it does
-- not export the Hash constructor
hashCombine :: Word64 -> Word64 -> Word64
hashCombine a b = rotate a 1 `xor` b

rollall :: Int -> B.ByteString -> [Int]
rollall n bs
    | n > B.length bs = []
    | otherwise = runST $ do
        hashes <- VUM.new n
        let inithash ch p
                | p == n - 1 = return (ch :: Word64)
                | otherwise = do
                    let hi = asWord64 $ hashWord8 (B.index bs p)
                        ch' = ch `hashCombine` hi
                    VUM.unsafeWrite hashes (p+1) hi
                    inithash ch' (p+1)
            initial_hash :: Word64
            initial_hash = (asWord64 $ hash ()) `hashCombine` (asWord64 $ hash n)
        VUM.unsafeWrite hashes 0 initial_hash
        ch <- inithash initial_hash 0
        let iter acc@(ch:_) p pix
                | p == B.length bs = return . tail . map (fromIntegral . toInteger) $ reverse acc
                | otherwise =  do
                    let ck = asWord64 (hash $ B.index bs p)
                    cl <- VUM.unsafeRead hashes pix
                    let ch' :: Word64
                        ch' = ch `hashCombine` (rotate cl n `xor` ck)
                    VUM.unsafeWrite hashes pix ck
                    iter (ch':acc) (p+1) (if pix + 1 == n then 0 else pix + 1)
        iter [ch] (n-1) 0


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

annotate :: Int -> Fasta -> (Fasta, (VU.Vector Int))
annotate hashSize fa@(Fasta _ fad) = (fa, VU.fromList (rollall hashSize fad))

buildHash :: (MonadIO m, MonadBase IO m) => Int -> C.Sink Fasta m SizedHash
buildHash nthreads = C.peek >>= \case
        Nothing -> error "Empty stream"
        Just first -> do
            let hashSize = faseqLength first
            SizedHash hashSize <$>
                    (C.conduitVector 4096
                    .| asyncMapC nthreads (V.map (annotate hashSize))
                    .| CL.fold insertmany IM.empty)
    where
        insertmany imap = V.foldl addHash imap
        addHash imap (faseq, hashes) = IM.alter (Just . (faseq:) . (fromMaybe [])) curk imap
            where
                hashes' = VU.toList hashes
                curk = headDef (head hashes') (filter (flip IM.notMember imap) hashes')

findOverlapsSingle nthreads = do
    first <- C.peek
    h <- liftIO HT.new
    whenJust first $ \faseq ->
        CC.conduitVector 4096
            .| asyncMapC nthreads (V.map (annotate (faseqLength faseq)))
            .| CC.concat
            .| findOverlapsSingle' h
  where
    findOverlapsSingle' :: MonadIO m => FastaIOHash -> C.Conduit (Fasta, VU.Vector Int) m B.ByteString
    findOverlapsSingle' imap = awaitJust $ \(faseq@(Fasta sid _), hashes) -> do
        let lookup1 :: Pair [Fasta] (Maybe Int) -> Int -> IO (Pair [Fasta] (Maybe Int))
            lookup1 (ccovered :!: mcurk) h = do
                val <- HT.lookup imap h
                return $! case val of
                    Nothing -> (ccovered :!: mcurk <|> Just h)
                    Just prevs -> (filter (`faContainedIn` faseq) prevs ++ ccovered :!: Nothing)
        (covered :!: minsertpos) <- liftIO $ VU.foldM lookup1 ([] :!: Nothing) hashes
        -- If we failed to find an empty slot for the sequence, just use first one
        let insertpos = fromMaybe (VU.head hashes) minsertpos

        forM_ covered $ \c ->
            C.yield (B.concat [seqheader c, "\tC\t", sid])
        liftIO $ HT.mutate imap insertpos (\curv -> (Just . (faseq:) . fromMaybe [] $ curv, ()))
        findOverlapsSingle' imap

findOverlapsAcross :: (MonadIO m, MonadBase IO m) => Int -> SizedHash -> C.Conduit Fasta m B.ByteString
findOverlapsAcross n (SizedHash hashSize imap) = CC.conduitVector 4096 .| asyncMapC n findOverlapsAcross' .| CC.concat
    where
        findOverlapsAcross' :: V.Vector Fasta -> V.Vector B.ByteString
        findOverlapsAcross' = V.concatMap findOverlapsAcross'1
        findOverlapsAcross'1 :: Fasta -> V.Vector B.ByteString
        findOverlapsAcross'1 faseq@(Fasta sid _) = V.fromList [B.concat [seqheader c, "\tC\t", sid] | c <- covered]
            where
                -- Starting in containers 0.5.8, we could use the
                -- IM.restrictKeys function to "lookup" all the keys at once
                hashes = rollall hashSize (seqdata faseq)
                candidates = flip concatMap hashes $ \h -> IM.findWithDefault [] h imap
                covered = filter (`faContainedIn` faseq) candidates

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
        ModeSingle -> C.runConduitRes $
            C.sourceFile (ifile opts)
                .| faConduit
                .| findOverlapsSingle nthreads
                .| C.unlinesAscii
                .| CB.sinkFileCautious (ofile opts)
        ModeAcross -> do
            h <- C.runConduitRes $
                C.sourceFile (ifile opts)
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
                    C.runConduitRes $
                        C.sourceFile fafile
                        .| faConduit
                        .| findOverlapsAcross nthreads h
                        .| C.unlinesAscii
                        .| C.sinkHandle hout
            putStrLn "Done."
