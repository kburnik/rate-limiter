from rate_limit import RateLimiter, Bucket
from sets import ImmutableSet
import unittest


class FakeClock(object):
  def __init__(self):
    self._timestamp = 0.0

  def tick(self, seconds):
    self._timestamp += seconds

  def time(self):
    return self._timestamp


class RateLimiterTestCase(unittest.TestCase):
  def setUp(self):
    self._clock = FakeClock()

  def create_limiter(self,
                     max_amount=1,
                     refill_time=1,
                     refill_amount=1):
    return RateLimiter(test_bucket=Bucket(max_amount=max_amount,
                                          refill_time=refill_time,
                                          refill_amount=refill_amount,
                                          clock=self._clock))

  def test_reduce_returnsTrueWhileHasTokens(self):
    self.assertTrue(self.create_limiter(max_amount=1).reduce())

  def test_reduce_returnsFalseWhenOverLimit(self):
    limiter = self.create_limiter(max_amount=1, refill_time=1, refill_amount=1)
    limiter.reduce()
    self.assertFalse(limiter.reduce())

  def test_reduce_refillsBucketAtNextReduce(self):
    limiter = self.create_limiter(max_amount=1, refill_time=1, refill_amount=1)
    limiter.reduce()
    self._clock.tick(seconds=1)
    self.assertTrue(limiter.reduce())

  def test_reduce_withNegativeRefillTimeNeverRefillsBucket(self):
    limiter = self.create_limiter(max_amount=10, refill_time=-1)
    limiter.reduce(tokens=10)
    self._clock.tick(seconds=100)
    self.assertFalse(limiter.reduce())

  def test_exhausted_emptyAfterInit(self):
    self.assertEqual(self.create_limiter().exhausted(), ImmutableSet())

  def test_exhausted_emptyWhileHasTokens(self):
    limiter = self.create_limiter(max_amount=1)
    limiter.reduce()
    self.assertEqual(limiter.exhausted(), ImmutableSet())

  def test_exhausted_returnsBucketNamesOverLimit(self):
    limiter = self.create_limiter(max_amount=1, refill_time=1, refill_amount=1)
    limiter.reduce()
    limiter.reduce()
    self.assertEqual(ImmutableSet(["test_bucket"]), limiter.exhausted())

  def test_get_refillsBucketUpToMaxAmountAfterEachRefillTime(self):
    limiter = self.create_limiter(max_amount=5, refill_time=1, refill_amount=1)
    limiter.reduce(5)
    bucket_state = []
    for i in range(7):
      bucket_state.append(limiter.get("test_bucket"))
      self._clock.tick(seconds=1)
    self.assertEqual(bucket_state, [0, 1, 2, 3, 4, 5, 5])


if __name__ == '__main__':
  unittest.main()
