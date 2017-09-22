import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import           Data.Conduit ((.|))
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.Ord (comparing)
import           Data.List (foldl')
import           Safe (atDef)
import           System.Exit (exitFailure)

import Data.BioConduit
import Algorithms.OutSort

newtype FASize = FASize { unwrapFASize :: Fasta }

instance Eq FASize where
    (FASize a) == (FASize b) = (a == b)

instance Ord FASize where
    compare = comparing $ \(FASize a) -> (faseqLength a, seqdata a, seqheader a)

maxBPSBlock :: Int
maxBPSBlock = 24 * 1000 * 1000 * 1000

outsortFasta :: [FilePath] -> FilePath -> IO ()
outsortFasta ifiles ofile = outsort
    (faConduit .| CL.map FASize)
    (CL.map unwrapFASize .| faWriteC)
    (isolateBySize (faseqLength . unwrapFASize) maxBPSBlock)
    (sequence_ $ map CB.sourceFile ifiles)
    (CB.sinkFileCautious ofile)

data CmdArgs = CmdArgs
                { argIfile :: FilePath
                , argOfile :: FilePath
                , argIFileList :: FilePath
                } deriving (Eq, Show)

data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | ListFile FilePath
                | Verbose
                deriving (Eq, Show)

options :: [OptDescr CmdFlags]
options =
    [ Option ['v'] ["verbose"] (NoArg Verbose)  "verbose mode"
    , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file"
    , Option ['F'] ["file-list"] (ReqArg ListFile "FILE") "Input is a list of files"
    , Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
    ]


parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs ifile ofile "") flags
    where
        (flags, args, _extraOpts) = getOpt Permute options argv
        ifile = atDef "" args 0
        ofile = atDef "" args 1

        p c (OutputFile o) = c { argOfile = o }
        p c (InputFile i) = c { argIfile = i }
        p c (ListFile f) = c { argIFileList = f }
        p c Verbose = c

main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    ifiles <- case (argIfile opts, argIFileList opts) of
            (ifile, "") -> return [ifile]
            ("", ffile) -> C.runConduitRes $
                                CB.sourceFile ffile
                                    .| CB.lines
                                    .| CL.map B8.unpack
                                    .| CL.consume
            _ -> do
                putStrLn "Cannot pass both input file and -F argument"
                exitFailure
    outsortFasta ifiles (argOfile opts)
