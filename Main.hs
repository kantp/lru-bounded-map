
{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.List
-- import Data.Function
import Data.Maybe
import Data.Hashable
import Text.Printf
import Control.Applicative
import Control.Monad
import Control.DeepSeq
import Criterion.Main
import Criterion.Config
import System.Exit

import qualified Data.Map.Strict as M
import qualified Data.HashMap.Strict as HM
import qualified Data.IntMap.Strict as IM
import qualified LRUBoundedMap_LinkedListHashMap as LBM_LLHM
import qualified LRUBoundedMap_DoubleMapBTree    as LBM_DMBT
import qualified LRUBoundedMap_CustomHAMT        as LBM_CHAMT
import qualified LRUBoundedMap_CustomHashedTrie  as LBM_CHT

criterionCfg :: Config
criterionCfg = defaultConfig { cfgPerformGC = ljust True
                             , cfgReport    = ljust "./report.html"
                             , cfgSamples   = ljust 5
                             }

main :: IO ()
main = do
    -- Load test set of representative keys
    keysL' <- B8.lines <$> B.readFile "keys.txt"
    -- Insert a known hash collision
    let keysL = "479199" : "662782" : drop 2 keysL'
    when (hash (keysL !! 0) /= hash (keysL !! 1)) $
        putStrLn "Warning: Hash collision not present, might not test this aspect properly"
    -- Find us some hash collisions
    {-
    let collisions = filter (\x -> length x /= 1)
                   . groupBy ((==)    `on` fst)
                   . sortBy  (compare `on` fst)
                   . map (\x -> (hash . B8.pack . show $ x, x))
                   $ ([1..1000000] :: [Int])
    unless (null collisions) $ do
        void $ printf "Collision(s) found: %s\n" $ show collisions
        exitFailure
    -}
    let numK  = printf "(%ik keys)" $ length keysL `div` 1000
        kvL   = zip keysL ([1..] :: [Int])
        -- Make initial maps for delete / lookup benchmarks
        mkDMS         = foldl' (\r (k, v) ->       M.insert k v r)         (M.empty)
        mkDHMS        = foldl' (\r (k, v) ->       HM.insert k v r)        (HM.empty)
        mkDIMS        = foldl' (\r (k, v) ->       IM.insert (hash k) v r) (IM.empty)
        mkLBM_LLHM5k  = foldl' (\r (k, v) -> fst $ LBM_LLHM.insert k v r)  (LBM_LLHM.empty  5000)
        mkLBM_LLHM1k  = foldl' (\r (k, v) -> fst $ LBM_LLHM.insert k v r)  (LBM_LLHM.empty  1000)
        mkLBM_DMBT5k  = foldl' (\r (k, v) -> fst $ LBM_DMBT.insert k v r)  (LBM_DMBT.empty  5000)
        mkLBM_DMBT1k  = foldl' (\r (k, v) -> fst $ LBM_DMBT.insert k v r)  (LBM_DMBT.empty  1000)
        mkLBM_CHAMT5k = foldl' (\r (k, v) -> fst $ LBM_CHAMT.insert k v r) (LBM_CHAMT.empty 5000)
        mkLBM_CHAMT1k = foldl' (\r (k, v) -> fst $ LBM_CHAMT.insert k v r) (LBM_CHAMT.empty 1000)
        mkLBM_CHT5k   = foldl' (\r (k, v) -> fst $ LBM_CHT.insert k v r)   (LBM_CHT.empty   5000)
        mkLBM_CHT1k   = foldl' (\r (k, v) -> fst $ LBM_CHT.insert k v r)   (LBM_CHT.empty   1000)
        lDMS          = mkDMS         kvL
        lDHMS         = mkDHMS        kvL
        lDIMS         = mkDIMS        kvL
        lLBM_LLHM5k   = mkLBM_LLHM5k  kvL
        lLBM_LLHM1k   = mkLBM_LLHM1k  kvL
        lLBM_DMBT5k   = mkLBM_DMBT5k  kvL
        lLBM_DMBT1k   = mkLBM_DMBT1k  kvL
        lLBM_CHAMT5k  = mkLBM_CHAMT5k kvL
        lLBM_CHAMT1k  = mkLBM_CHAMT1k kvL
        lLBM_CHT5k    = mkLBM_CHT5k   kvL
        lLBM_CHT1k    = mkLBM_CHT1k   kvL
    -- Some basic tests for the LRU CT map
    case LBM_CHT.valid $ mkLBM_CHT5k kvL of
        Just err -> (putStrLn $ "mkLBM_CHT5k.valid: " ++ err) >> exitFailure
        Nothing  -> return ()
    case LBM_CHT.valid $ mkLBM_CHT1k kvL of
        Just err -> (putStrLn $ "mkLBM_CHT1k.valid: " ++ err) >> exitFailure
        Nothing  -> return ()
    forM_ (LBM_LLHM.view $ mkLBM_LLHM5k kvL) $ \(k, v) ->
        case LBM_CHT.lookup k (mkLBM_CHT5k kvL) of
          (_, Just v') -> when (v /= v') $ do
                              putStrLn $ "mkLBM_CHT5k invalid value for key: " ++ B8.unpack k
                              exitFailure
          (_, Nothing) -> (putStrLn $ "mkLBM_CHT5k missing key: " ++ B8.unpack k) >> exitFailure
    when (LBM_LLHM.size (mkLBM_LLHM5k kvL) /= LBM_CHT.size (mkLBM_CHT5k kvL)) $
        (putStrLn "mkLBM_CHT5k size mismatch") >> exitFailure
    forM_ (LBM_LLHM.view $ mkLBM_LLHM1k kvL) $ \(k, v) ->
        case LBM_CHT.lookup k (mkLBM_CHT1k kvL) of
          (_, Just v') -> when (v /= v') $ do
                              putStrLn $ "mkLBM_CHT1k invalid value for key: " ++ B8.unpack k
                              exitFailure
          (_, Nothing) -> (putStrLn $ "mkLBM_CHT1k missing key: " ++ B8.unpack k) >> exitFailure
    when (LBM_LLHM.size (mkLBM_LLHM1k kvL) /= LBM_CHT.size (mkLBM_CHT1k kvL)) $
        (putStrLn "mkLBM_CHT1k size mismatch") >> exitFailure
    let allDeleted = foldl' (\r k -> fst $ LBM_CHT.delete k r) (mkLBM_CHT5k kvL) keysL
     in do when ((fst $ LBM_CHT.size allDeleted) /= 0) $
              (putStrLn "mkLBM_CHT5k delete failed") >> exitFailure
           case LBM_CHT.valid allDeleted of
               Just err -> (putStrLn $ "delete: mkLBM_CHT5k.valid: " ++ err) >> exitFailure
               Nothing  -> return ()
    let lookups    = (map (fst) . take 100 . drop 4000 $ kvL) ++
                     (map (fst) . take 100             $ kvL)
        insertions = [(B8.pack $ show i, i) | i <- [1..50]]
        reference  = sort . LBM_LLHM.view $
                       foldl' (\r (k, v) -> fst $ LBM_LLHM.insert k v r)
                              ( foldl' (\r k -> fst $ LBM_LLHM.lookup k r)
                                       (mkLBM_LLHM1k kvL)
                                       lookups
                              )
                              insertions
        testMap    = foldl' (\r (k, v) -> fst $ LBM_CHT.insert k v r)
                            ( foldl' (\r k -> fst $ LBM_CHT.lookup k r)
                                     (mkLBM_CHT1k kvL)
                                     lookups
                            )
                            insertions
        test       = sort $ LBM_CHT.toList testMap
     in do case LBM_CHT.valid testMap of
               Just err -> do putStrLn $ "lookup / insert / delete test valid: " ++ err
                              exitFailure
               Nothing  -> return ()
           when (test /= reference) $ do
              putStrLn "mkLBM_CHT1k lookup / insert / delete comparison failed"
              print $ reference \\ test
              exitFailure
    -- Make sure we build the initial maps
    (lDMS        ) `deepseq`
      (lDHMS       ) `deepseq`
      (lDIMS       ) `deepseq`
      (lLBM_LLHM5k ) `deepseq`
      (lLBM_LLHM1k ) `deepseq`
      (lLBM_DMBT5k ) `deepseq`
      (lLBM_DMBT1k ) `deepseq`
      (lLBM_CHAMT5k) `deepseq`
      (lLBM_CHAMT1k) `deepseq`
      (lLBM_CHT5k  ) `deepseq`
      (lLBM_CHT1k  ) `deepseq`
      -- Run criterion benchmarks
      defaultMainWith
        criterionCfg
        (return ())
        [
          bgroup ("insert (w/o LRU update)" ++ numK)
          [
            bench "Data.Map.Strict"                    . nf (mkDMS)         $ kvL
          , bench "Data.HashMap.Strict"                . nf (mkDHMS)        $ kvL
          , bench "Data.IntMap.Strict"                 . nf (mkDIMS)        $ kvL
          , bench "LBM_CustomHAMT"                     . nf (mkLBM_CHAMT5k) $ kvL
          ]
        , bgroup ("insert (w/ LRU update)" ++ numK)
          [
          {- -- Disabled, too slow (uses O(n) size call)
          , bench "LBM_LinkedListHashMap (limit 5k)"   . nf (mkLBM_LLHM5k)  $ kvL
          , bench "LBM_LinkedListHashMap (limit 1k)"   . nf (mkLBM_LLHM1k)  $ kvL
          -}
            bench "LBM_DoubleMapBTree (limit 5k)"      . nf (mkLBM_DMBT5k)  $ kvL
          , bench "LBM_DoubleMapBTree (limit 1k)"      . nf (mkLBM_DMBT1k)  $ kvL
          , bench "LBM_CustomHashedTrie (limit 5k)"    . nf (mkLBM_CHT5k)   $ kvL
          , bench "LBM_CustomHashedTrie (limit 1k)"    . nf (mkLBM_CHT1k)   $ kvL
          ]
        , bgroup ("delete (w/o LRU update)" ++ numK)
          [
            bench "Data.Map.Strict" . nf
              (foldl' (\r k -> M.delete k r) lDMS) $ keysL
          , bench "Data.HashMap.Strict" . nf
              (foldl' (\r k -> HM.delete k r) lDHMS) $ keysL
          , bench "Data.IntMap.Strict" . nf
              (foldl' (\r k -> IM.delete (hash k) r) lDIMS) $ keysL
          , bench "LBM_CustomHAMT" . nf
              (foldl' (\r k -> fst $ LBM_CHAMT.delete k r) lLBM_CHAMT5k) $ keysL
          ]
        , bgroup ("delete (w/ LRU update)" ++ numK)
          [
            bench "LBM_LinkedListHashMap (limit 5k)" . nf
              (foldl' (\r k -> fst $ LBM_LLHM.delete k r) lLBM_LLHM5k) $ keysL
          , bench "LBM_LinkedListHashMap (limit 1k)" . nf
              (foldl' (\r k -> fst $ LBM_LLHM.delete k r) lLBM_LLHM1k) $ keysL
          , bench "LBM_DoubleMapBTree (limit 5k)" . nf
              (foldl' (\r k -> LBM_DMBT.delete k r) lLBM_DMBT5k) $ keysL
          , bench "LBM_DoubleMapBTree (limit 1k)" . nf
              (foldl' (\r k -> LBM_DMBT.delete k r) lLBM_DMBT1k) $ keysL
          , bench "LBM_CustomHashedTrie (limit 5k)" . nf
              (foldl' (\r k -> fst $ LBM_CHT.delete k r) lLBM_CHT5k) $ keysL
          , bench "LBM_CustomHashedTrie (limit 1k)" . nf
              (foldl' (\r k -> fst $ LBM_CHT.delete k r) lLBM_CHT1k) $ keysL
          ]
        , bgroup ("lookup (w/o LRU update) " ++ numK)
          [
            bench "Data.Map.Strict" . nf
              (foldl' (\r k -> (r +) . fromJust . M.lookup k $ lDMS) 0) $ keysL
          , bench "Data.HashMap.Strict" . nf
              (foldl' (\r k -> (r +) . fromJust . HM.lookup k $ lDHMS) 0) $ keysL
          , bench "Data.IntMap.Strict" . nf
              (foldl' (\r k -> (r +) . fromJust . IM.lookup (hash k) $ lDIMS) 0) $ keysL
          , bench "LBM_CustomHashedTrie (limit 5k)" . nf
              (foldl' (\r k -> (r +)
                  . fromMaybe 0 . LBM_CHT.lookupNoLRU k $ lLBM_CHT5k) 0) $ keysL
          , bench "LBM_CustomHashedTrie (limit 1k)" . nf
              (foldl' (\r k -> (r +)
                  . fromMaybe 0 . LBM_CHT.lookupNoLRU k $ lLBM_CHT1k) 0) $ keysL
          , bench "LBM_CustomHAMT" . nf
            (foldl' (\r k -> (r +)
                . fromMaybe 0 . LBM_CHAMT.lookupNoLRU k $ lLBM_CHAMT5k) 0) $ keysL
          ]
        , bgroup ("lookup (w/ LRU update) " ++ numK)
          [
            lkBench "LBM_LinkedListHashMap (limit 5k)" (LBM_LLHM.lookup) lLBM_LLHM5k keysL
          , lkBench "LBM_LinkedListHashMap (limit 1k)" (LBM_LLHM.lookup) lLBM_LLHM1k keysL
          , lkBench "LBM_DoubleMapBTree (limit 5k)"    (LBM_DMBT.lookup) lLBM_DMBT5k keysL
          , lkBench "LBM_DoubleMapBTree (limit 1k)"    (LBM_DMBT.lookup) lLBM_DMBT1k keysL
          --, lkBench "LBM_CustomHAMT (limit 5k)"        (LBM_CHAMT.lookup) lLBM_CHAMT5k keysL
          --, lkBench "LBM_CustomHAMT (limit 1k)"        (LBM_CHAMT.lookup) lLBM_CHAMT1k keysL
          , lkBench "LBM_CustomHashedTrie (limit 5k)"  (LBM_CHT.lookup)  lLBM_CHT5k  keysL
          , lkBench "LBM_CustomHashedTrie (limit 1k)"  (LBM_CHT.lookup)  lLBM_CHT1k  keysL
          ]
        ]
    where {-# INLINE lkBench #-}
          lkBench name lk fullMap list = bench name . nf
              (foldl' (\(r, a) k ->
                  (\(r', a') ->
                    a `seq` (r', a + fromMaybe 0 a')) $
                  lk k r)
                (fullMap, 0)) $
                list

