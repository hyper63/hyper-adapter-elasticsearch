import { base64Encode, crocks, R } from "./deps.js";

const { Async } = crocks;
const { ifElse, assoc, pipe, identity } = R;

// TODO: Tyler. wrap with opionated approach like before with https://github.com/vercel/fetch
const asyncFetch = (fetch) => Async.fromPromise(fetch);

const createHeaders = (username, password) =>
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

const handleResponse = (pred) =>
  ifElse(
    (res) => pred(res),
    (res) =>
      Async.of(res)
        .chain(Async.fromPromise((res) => res.json())),
    (res) => Async.Rejected(res),
  );

export { asyncFetch, createHeaders, handleResponse };
