#!/usr/bin/env stack
-- stack --resolver lts-9.4 script
{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import Control.Monad
import System.Environment
import System.Console.GetOpt
import Data.Maybe
import Data.List

import Data.BioConduit

splitFiles :: FilePath -> FilePath -> Int -> IO ()
splitFiles ifile ofileBase nSeqs = C.runConduitRes $
        CB.sourceFile ifile
            .| faConduit
            .| splitWriter
    where
        splitWriter = splitWriter' (0 :: Int)
        splitWriter' n = do
            CL.isolate nSeqs
                .| faWriteC
                .| CB.sinkFileCautious (ofileBase ++ "." ++ show n ++ ".fna")
            whenM isMore $
                splitWriter' $! n + 1
        isMore = isJust <$> CC.peek
        whenM cond act = do
            cval <- cond
            if cval
                then act
                else return ()

data CmdArgs = CmdArgs
                { ifileArg  :: FilePath
                , ofileArg :: FilePath
                } deriving (Show)


data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                deriving (Eq, Show)

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs "" "") flags
    where
        (flags, [], []) = getOpt Permute options argv
        options =
            [ Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
            , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file to check"
            ]
        p c (OutputFile f) = c { ofileArg = f }
        p c (InputFile f) = c { ifileArg = f }

main :: IO ()
main = do
    CmdArgs ifile ofileBase <- parseArgs <$> getArgs
    splitFiles ifile ofileBase (1000 * 1000)
