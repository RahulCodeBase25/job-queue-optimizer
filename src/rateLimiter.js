// Token bucket: allows short bursts up to `capacity`, then a steady
// `perSecond` rate. take() returns 0 if a token was available, otherwise
// how many milliseconds until the next one.
class TokenBucket {
  constructor({ perSecond, capacity = perSecond }, now = Date.now) {
    this.rate = perSecond / 1000;
    this.capacity = capacity;
    this.tokens = capacity;
    this.now = now;
    this.last = now();
  }

  refill() {
    const t = this.now();
    this.tokens = Math.min(this.capacity, this.tokens + (t - this.last) * this.rate);
    this.last = t;
  }

  take() {
    this.refill();
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return 0;
    }
    return Math.ceil((1 - this.tokens) / this.rate);
  }
}

module.exports = { TokenBucket };
