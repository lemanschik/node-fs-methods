export const fromCallback = (fn) => {
  return Object.defineProperty(function (...args) {
    if (typeof args.at(-1) === 'function') {
      fn.apply(this, args);
    } else {
      return new Promise((res, rej) => {
        fn.apply(this, [...args, (err, done) => err ? rej(err) : res(done)]);
      })
    }
  }, 'name', { value: fn.name })
};

export const fromPromise = (fn) => {
  return Object.defineProperty(function (...args) {
    const cb = args.at(-1);
    if (typeof cb !== 'function') { 
      return fn.apply(this, args);
    } else {
      fn.apply(this, args.slice(0,-1)).then(r => cb(null, r), cb);
    }
  }, 'name', { value: fn.name })
}
