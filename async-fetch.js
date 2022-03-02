import { base64Encode, crocks, R } from "./deps.js";
import { toEsErr } from "./utils.js";

const { Async } = crocks;
const {
  ifElse,
  assoc,
  pipe,
  identity,
  propOr,
  compose,
  tap,
} = R;

// TODO: Tyler. wrap with opionated approach like before with https://github.com/vercel/fetch
export const asyncFetch = (fetch) => Async.fromPromise(fetch);

export const createHeaders = (username, password) =>
  pipe(
    assoc("Content-Type", "application/json"),
    assoc("Accept", "application/json"),
    ifElse(
      () => username && password,
      assoc(
        "authorization",
        `Basic ${
          base64Encode(new TextEncoder().encode(username + ":" + password))
        }`,
      ),
      identity,
    ),
  )({});

export const handleResponse = (pred) =>
  ifElse(
    (res) => pred(res),
    (res) =>
      Async.of(res)
        .chain(Async.fromPromise((res) => res.json())),
    (res) =>
      Async.of(res)
        .chain(Async.fromPromise((res) => res.json()))
        .map(tap(console.log))
        /**
         * Elasticsearch errors have the format:
         * { error: { reason: string }, status: number }
         */
        .map((body) =>
          compose(
            (err) => toEsErr(err, res.status),
            assoc("status", body.status),
            propOr(body, "error"),
          )(body)
        )
        .chain(Async.Rejected),
  );
