import { combineLatest, Observable, Subject } from "rxjs";

type Key = string | number;
type Relation = (keyA: Key, keyB: Key) => boolean;

class State<T, U> {
  source: Map<Key, T>;
  target: Map<Key, U>;

  constructor() {
    this.source = new Map();
    this.target = new Map();
  }

  setSource(key: Key, val: T) {
    this.source.set(key, val);
  }

  setTarget(key: Key, val: U) {
    this.target.set(key, val);
  }

  *innerJoin(relation: Relation): Generator<[T, U, Key, Key]> {
    for (const [sKey, sVal] of this.source) {
      for (const [tKey, tVal] of this.target) {
        if (relation(sKey, tKey)) {
          yield [sVal, tVal, sKey, tKey];
          this.source.delete(sKey);
          this.target.delete(tKey);
        }
      }
    }
  }

  *leftJoin(relation: Relation): Generator<[T, U | null, Key, Key | null]> {
    for (const [sKey, sVal] of this.source) {
      let matched = false;
      for (const [tKey, tVal] of this.target) {
        if (relation(sKey, tKey)) {
          matched = true;
          yield [sVal, tVal, sKey, tKey];
          this.source.delete(sKey);
          this.target.delete(tKey);
        }
      }
      if (!matched) {
        yield [sVal, null, sKey, null];
      }
    }
  }

  clear() {
    this.source.clear();
    this.target.clear();
  }
}

export const innerJoin =
  <T, U>(target: Observable<U>, on: (a: T, b: U) => [Key, Key, Relation]) =>
  (source: Observable<T>) => {
    const state = new State<T, U>();
    const subject = new Subject<[T, U, Key, Key]>();
    combineLatest([source, target]).subscribe({
      next: (vals: [T, U]) => {
        const [sourceKey, targetKey, relation] = on(...vals);
        state.setSource(sourceKey, vals[0]);
        state.setTarget(targetKey, vals[1]);
        for (const nextVals of state.innerJoin(relation)) {
          subject.next(nextVals);
        }
      },
      complete: () => {
        state.clear();
      },
    });
    return subject;
  };

export const leftJoin =
  <T, U>(target: Observable<U>, on: (a: T, b: U) => [Key, Key, Relation]) =>
  (source: Observable<T>) => {
    const state = new State<T, U>();
    const subject = new Subject<[T, U | null, Key, Key | null]>();
    combineLatest([source, target]).subscribe({
      next: (vals: [T, U]) => {
        const [sourceKey, targetKey, relation] = on(...vals);
        state.setSource(sourceKey, vals[0]);
        state.setTarget(targetKey, vals[1]);
        for (const nextVals of state.leftJoin(relation)) {
          subject.next(nextVals);
        }
      },
      complete: () => {
        state.clear();
      },
    });
    return subject;
  };
