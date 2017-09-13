import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import           Data.Conduit ((.|))
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.Ord (comparing)
import           Data.List (foldl')

import Data.BioConduit
import Algorithms.OutSort

newtype FASize = FASize { unwrapFASize :: Fasta }

instance Eq FASize where
    (FASize a) == (FASize b) = (a == b)

instance Ord FASize where
    compare = comparing $ \(FASize a) -> (faseqLength a, seqdata a, seqheader a)

outsortFasta :: FilePath -> FilePath -> IO ()
outsortFasta ifile ofile = outsort
    (faConduit .| CL.map FASize)
    (CL.map unwrapFASize .| faWriteC)
    (isolateBySize (faseqLength . unwrapFASize) 2000000)
    (CB.sourceFile ifile)
    (CB.sinkFileCautious ofile)

data CmdArgs = CmdArgs
                { argIfile :: FilePath
                , argOfile :: FilePath
                } deriving (Eq, Show)

data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | Verbose
                deriving (Eq, Show)

options :: [OptDescr CmdFlags]
options =
    [ Option ['v'] ["verbose"] (NoArg Verbose)  "verbose mode"
    , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file"
    , Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
    ]


parseArgs :: [String] -> CmdArgs
parseArgs [iarg, oarg] = CmdArgs iarg oarg
parseArgs argv = foldl' p (CmdArgs "" "") flags
    where
        (flags, [], _extraOpts) = getOpt Permute options argv
        p c (OutputFile o) = c { argOfile = o }
        p c (InputFile i) = c { argIfile = i }
        p c Verbose = c

main :: IO ()
main = do
    CmdArgs ifile ofile <- parseArgs <$> getArgs
    outsortFasta ifile ofile
