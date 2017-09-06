#!/usr/bin/env stack
-- stack --resolver lts-8.13 script
{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as C
import qualified Data.Conduit.Zlib as CZ
import qualified Data.Conduit as C
import           Data.Conduit ((.|), ($$+), ($$++))
import qualified Data.Set as S
import qualified Control.Concurrent.Async as A
import System.Environment
import Control.Monad
import System.Directory
import System.IO
import System.Console.GetOpt
import Data.Maybe
import Data.Ord
import Data.List
import Control.Exception
import Control.Monad.Primitive
import Control.Monad.IO.Class
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.AmericanFlag as VA
import qualified Data.Vector.Mutable as VM
import Data.Vector.Algorithms.Search
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Vector.Generic.Mutable as VGM
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Class
import Data.Word
import Data.Hash
import Data.Hash.Rolling
import qualified Data.IntMap.Strict as IM

import Data.BioConduit
import Utils.Conduit

type FastaMap = IM.IntMap [Fasta]

faContained :: Fasta -> Fasta -> Bool
faContained (Fasta _ da) (Fasta _ db) = B.isInfixOf da db

headDef v [] = v
headDef _ (x:_) = x

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

allHashes :: Int -> Fasta -> [Int]
allHashes hashSize (Fasta _ faseq) = toInt <$> hashrest (B.unpack cont) initH
  where
    (beg,cont) = B.splitAt hashSize faseq
    initH :: RollingHash Word8
    initH = B.foldl' addAndRoll (rollingHash hashSize) beg
    hashrest [] rh = [currentHash rh]
    hashrest (c:cs) rh = (currentHash rh:hashrest cs (addAndRoll rh c))
    toInt :: Hash -> Int
    toInt = fromIntegral . toInteger . asWord64

data SizedHash = SizedHash !Int !FastaMap


buildHash :: (Monad m) => C.Sink Fasta m SizedHash
buildHash = CL.fold addHash (SizedHash 0 IM.empty)
    where
        addHash (SizedHash h imap) faseq@(Fasta _ sseq) = SizedHash hashSize (IM.alter (Just . (faseq:) . (fromMaybe [])) curk imap)
            where
                hashSize
                    | h == 0 = B.length sseq
                    | otherwise = h
                hashes = allHashes hashSize faseq
                curk = headDef (head hashes) (filter (flip IM.notMember imap) hashes)

findRepeats = findRepeats' Nothing (IM.empty :: FastaMap)
  where
    findRepeats' :: (MonadIO m) => Maybe Int -> FastaMap -> C.Conduit Fasta m B.ByteString
    findRepeats' hs imap = C.await >>= \case
        Nothing -> return ()
        Just faseq@(Fasta sid sseq) -> do
            let hashSize = fromMaybe (B.length sseq) hs
                hashes :: [Int]
                hashes = allHashes hashSize faseq
                candidates :: [[Fasta]]
                available :: [Maybe Int]
                (candidates, available) = unzip $ flip map hashes $ \h -> case IM.lookup h imap of
                    Nothing -> ([] :: [Fasta], Just h)
                    Just prevs -> (prevs, Nothing)

                covered = filter (`faContained` faseq) (concat candidates)
                curk = headDef (head hashes) $ catMaybes available
            when (not $ null covered) $
                C.yield $ B.intercalate "\t" (sid:map seqheader covered)
            findRepeats' (Just hashSize) (IM.alter (Just . (faseq:) . (fromMaybe [])) curk imap)

vMapMaybe :: (a -> Maybe b) -> V.Vector a -> V.Vector b
vMapMaybe f v = V.unfoldr loop 0
    where
        loop i
           | i < V.length v = case f (v V.! i) of
                                Just val -> Just (val, i + 1)
                                Nothing -> loop (i + 1)
           | otherwise = Nothing

--findRepeatsIn :: (MonadIO m, MonadBase IO m) => Int -> SizedHash -> C.Conduit Fasta m B.ByteString
findRepeatsIn n (SizedHash hashSize imap) = CC.conduitVector 4096 .| asyncMapC n findRepeatsIn' .| CC.concat
    where
        findRepeatsIn' :: V.Vector Fasta -> V.Vector B.ByteString
        findRepeatsIn' = vMapMaybe findRepeatsIn'1
        findRepeatsIn'1 :: Fasta -> Maybe B.ByteString
        findRepeatsIn'1 faseq@(Fasta sid sseq)
                | null covered = Nothing
                | otherwise = Just $ B.intercalate "\t" (sid:map seqheader covered)
            where
                hashes = allHashes hashSize faseq
                candidates = concat $ flip map hashes $ \h -> IM.findWithDefault [] h imap
                covered = filter (`faContained` faseq) candidates

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

parseArgs [oarg, iarg] = CmdArgs iarg oarg ModeSingle False 8 []
parseArgs argv = foldl' p (CmdArgs "" "" ModeSingle False 8 []) flags
    where
        (flags, [], _extraOpts) = getOpt Permute options argv
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
    case mode opts of
        ModeSingle -> C.runConduitRes $
            C.sourceFile (ifile opts)
                .| faConduit
                .| findRepeats
                .| C.unlinesAscii
                .| C.sinkFile (ofile opts)
        ModeAcross -> do
            h <- C.runConduitRes $
                C.sourceFile (ifile opts)
                    .| faConduit
                    .| buildHash
            putStrLn "Built initial hash"
            extraFiles <-
                C.runConduitRes $
                    C.sourceFile (extraFilesFilesFrom opts)
                    .| C.lines
                    .| CL.map B8.unpack
                    .| CL.consume
            withFile (ofile opts) WriteMode $ \hout ->
                forM_ extraFiles $ \fafile -> do
                    putStrLn ("Handling file "++fafile)
                    C.runConduitRes $
                        C.sourceFile fafile
                        .| faConduit
                        .| findRepeatsIn (nJobs opts) h
                        .| C.unlinesAscii
                        .| C.sinkHandle hout
            putStrLn "Done."
