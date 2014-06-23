{-# LANGUAGE BangPatterns #-}
module PSQueues_LRUCache
    ( Map
    , empty
    , insert
    , lookup
    , lookupNoLRU
    , delete
    ) where


import           Prelude hiding (lookup)
import           Control.DeepSeq (NFData (..))
import qualified Data.HashPSQ as HashPSQ
import           Data.Hashable (Hashable)

type Tick = Int
type Size = Int

data Map k v = Map
    { mLimit :: !Size
    , mTick  :: !Size
    , mSize  :: !Size
    , mQueue :: !(HashPSQ.HashPSQ k Int v)
    }

instance (NFData k, NFData v) => NFData (Map k v) where
    rnf (Map l t s q) = rnf l `seq` rnf t `seq` rnf s `seq` rnf q

empty :: Int -> Map k v
empty limit
    | limit < 1                    = error
        "limit for LRUCache needs to be >= 1"
    | limit > fromIntegral maxTick = error $
        "limit for LRUBoundedMap needs to be <= " ++ show maxTick
    | otherwise                    = Map
        { mLimit = limit
        , mTick  = minBound
        , mSize  = 0
        , mQueue = HashPSQ.empty
        }
  where -- Probably doesn't make much sense to go higher than this
    maxTick = (maxBound :: Tick) `div` 2

{-# INLINEABLE insert #-}
insert
    :: (Hashable k, Ord k) => k -> v -> Map k v -> (Map k v, Maybe (k, v))
insert k x m@(Map _ t s q) =
    case HashPSQ.insertView k t x q of
        (Just (_, x'), q') -> (increaseTick m {mQueue = q'}, Just (k, x'))
        (Nothing,      q') -> popOldestIfAtSizeLimit $
            compactIfAtTickLimit $ increaseTick m
                { mQueue = q'
                , mSize  = s + 1
                }

{-# INLINEABLE increaseTick #-}
increaseTick :: Map k v -> Map k v
increaseTick m = m {mTick = mTick m + 1}

{-# INLINE compactIfAtTickLimit #-}
compactIfAtTickLimit :: (Hashable k, Ord k) => Map k v -> Map k v
compactIfAtTickLimit m
    | mTick m >= maxBound = compactTicks m
    | otherwise           = m

compactTicks :: (Hashable k, Ord k) => Map k v -> Map k v
compactTicks m = go m . empty $ mLimit m
  where
    go msrc mdst =
        let (msrc', mkv) = popOldest msrc
        in  case mkv of
                Just (k, v) -> go msrc' . fst $ insert k v mdst
                Nothing     -> mdst

popOldest
    :: (Hashable k, Ord k) => Map k v -> (Map k v, Maybe (k, v))
popOldest m = case HashPSQ.minView (mQueue m) of
    Nothing           -> (m, Nothing)
    Just (k, _, x, q) ->
        let m' = m {mSize  = mSize m - 1, mQueue = q}
        in  (m', Just (k, x))

{-# INLINEABLE popOldestIfAtSizeLimit #-}
popOldestIfAtSizeLimit
    :: (Hashable k, Ord k) => Map k v -> (Map k v, Maybe (k, v))
popOldestIfAtSizeLimit m
    | mSize m <= mLimit m = (m, Nothing)
    | otherwise           = popOldest m

{-# INLINEABLE lookup #-}
lookup
    :: (Hashable k, Ord k) => k -> Map k v -> (Map k v, Maybe v)
lookup k m@(Map _ t _ q) =
    case HashPSQ.alter f k q of
        (mbX, q') -> (compactIfAtTickLimit (increaseTick m {mQueue = q'}), mbX)
  where
    f Nothing       = (Nothing, Nothing)
    f (Just (_, x)) = (Just x,  Just (t, x))

{-# INLINEABLE lookupNoLRU #-}
lookupNoLRU
    :: (Hashable k, Ord k) => k -> Map k v -> Maybe v
lookupNoLRU k m = case HashPSQ.lookup k (mQueue m) of
    Nothing     -> Nothing
    Just (_, x) -> Just x

{-# INLINEABLE delete #-}
delete
    :: (Hashable k, Ord k) => k -> Map k v -> (Map k v, Maybe v)
delete k m = case HashPSQ.deleteView k (mQueue m) of
    Nothing        -> (m,                                   Nothing)
    Just (_, x, q) -> (m {mQueue = q, mSize = mSize m - 1}, Just x)
