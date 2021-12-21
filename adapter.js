import { crocks, R } from "./deps.js";

import {
  bulkPath,
  createIndexPath,
  deleteIndexPath,
  getDocPath,
  indexDocPath,
  queryPath,
  removeDocPath,
  updateDocPath,
} from "./paths.js";

const {
  set,
  lensProp,
  pluck,
  reduce,
  always,
  pipe,
  map,
  join,
  concat,
  flip,
  toPairs,
} = R;

const { Async } = crocks;

/**
 * @typedef {Object} IndexInfo
 * @property {string} index - index name
 * @property {Object} mappings
 *
 * @typedef {Object} BulkSearchDoc
 * @property {boolean} ok
 * @property {string} [msg]
 * @property {Array<any>} results
 *
 * @typedef {Object} SearchDoc
 * @property {string} index
 * @property {string} key
 * @property {Object} doc
 *
 * @typedef {Object} SearchInfo
 * @property {string} index
 * @property {string} key
 *
 * @typedef {Object} SearchOptions
 * @property {Array<string>} fields
 * @property {Object} boost
 * @property {boolean} prefix
 *
 * @typedef {Object} SearchQuery
 * @property {string} index
 * @property {string} query
 * @property {SearchOptions} [options]
 *
 * @typedef {Object} Response
 * @property {boolean} ok
 * @property {string} [msg]
 *
 * @typedef {Object} ResponseWithResults
 * @property {boolean} ok
 * @property {string} [msg]
 * @property {Array<any>} results
 *
 * @typedef {Object} ResponseWithMatches
 * @property {boolean} ok
 * @property {string} [msg]
 * @property {Array<any>} matches
 */

const handleRejectedResponse = (res) =>
  Async.of(res)
    .chain(Async.fromPromise((res) => res.json()))
    .bichain(
      () => Async.Rejected({ ok: false, status: res.status }), // not json body, so no message
      (body) =>
        Async.Rejected({
          ok: false,
          status: res.status,
          msg: JSON.stringify(body),
        }),
    );

/**
 * TODO:
 * - Sanitize inputs ie. index names
 * - Map Port api to Elasticsearch api for creating an index
 * - Enable monitoring ie. with bimap(tap(console.err), tap(console.log))
 * - How to support different versions of Elasticsearch?
 * - ? Should we expose Elasticsearch response in result as res?
 */
export default function ({ config, asyncFetch, headers, handleResponse }) {
  /**
   * @param {IndexInfo}
   * @returns {Promise<Response>}
   */
  function createIndex({ index, mappings }) {
    const properties = mappings.fields.reduce(
      (a, f) => set(lensProp(f), { type: "text" }, a),
      {},
    );
    console.log("adapter-elasticsearch", properties);

    return asyncFetch(
      createIndexPath(config.origin, index),
      {
        headers,
        method: "PUT",
        body: JSON.stringify({
          mappings: { properties },
        }),
      },
    )
      .chain(handleResponse((res) => res.status < 400))
      .bichain(
        handleRejectedResponse,
        always(Async.Resolved({ ok: true })),
      )
      .toPromise();
  }

  /**
   * @param {string} index
   * @returns {Promise<Response>}
   */
  function deleteIndex(index) {
    return asyncFetch(
      deleteIndexPath(config.origin, index),
      {
        headers,
        method: "DELETE",
      },
    )
      .chain(
        handleResponse((res) => res.status === 200),
      )
      .bichain(
        handleRejectedResponse,
        always(Async.Resolved({ ok: true })),
      )
      .toPromise();
  }

  /**
   * @param {SearchDoc}
   * @returns {Promise<Response>}
   */
  function indexDoc({ index, key, doc }) {
    return asyncFetch(
      indexDocPath(config.origin, index, key),
      {
        headers,
        method: "PUT",
        body: JSON.stringify(doc),
      },
    )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bichain(
        handleRejectedResponse,
        always(Async.Resolved({ ok: true })),
      )
      .toPromise();
  }

  /**
   * @param {SearchInfo}
   * @returns {Promise<Response>}
   */
  function getDoc({ index, key }) {
    return asyncFetch(
      getDocPath(config.origin, index, key),
      {
        headers,
        method: "GET",
      },
    )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bichain(
        handleRejectedResponse,
        (res) => Async.Resolved({ ok: true, key, doc: res }),
      )
      .toPromise();
  }

  /**
   * @param {SearchDoc}
   * @returns {Promise<Response>}
   */
  function updateDoc({ index, key, doc }) {
    return asyncFetch(
      updateDocPath(config.origin, index, key),
      {
        headers,
        method: "PUT",
        body: JSON.stringify(doc),
      },
    )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bichain(
        handleRejectedResponse,
        always(Async.Resolved({ ok: true })),
      )
      .toPromise();
  }

  /**
   * @param {SearchInfo}
   * @returns {Promise<Response>}
   */
  function removeDoc({ index, key }) {
    return asyncFetch(
      removeDocPath(config.origin, index, key),
      {
        headers,
        method: "DELETE",
      },
    )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bichain(
        handleRejectedResponse,
        always(Async.Resolved({ ok: true })),
      )
      .toPromise();
  }

  /**
   * @param {BulkIndex}
   * @returns {Promise<ResponseWithResults>}
   *
   * TODO: maybe we could just Promise.all a map to indexDoc()?
   */
  function bulk({ index, docs }) {
    return asyncFetch(
      bulkPath(config.origin),
      {
        headers,
        method: "POST",
        // See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-example
        body: pipe(
          reduce(
            (
              arr,
              doc,
            ) => [...arr, { index: { _index: index, _id: doc.id } }, doc],
            [],
          ),
          // stringify each object in arr
          map(JSON.stringify.bind(JSON)),
          join("\n"),
          // Bulk payload must end with a newline
          flip(concat)("\n"),
        )(docs),
      },
    )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bichain(
        handleRejectedResponse,
        (res) => Async.Resolved({ ok: true, results: res.items }),
      )
      .toPromise();
  }

  /**
   * @param {SearchQuery}
   * @returns {Promise<ResponseWithMatches>}
   */
  function query({ index, q: { query, fields, filter } }) {
    return asyncFetch(
      queryPath(config.origin, index),
      {
        headers,
        method: "POST",
        // anything undefined will not be stringified, so this shorthand works
        body: JSON.stringify({
          query: {
            bool: {
              must: {
                multi_match: {
                  query,
                  // See https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html#query-dsl-match-query-fuzziness
                  fuzziness: "AUTO",
                  fields,
                },
              },
              filter: toPairs(filter).map(
                ([key, value]) => ({ term: { [key]: value } }),
              ),
            },
          },
        }),
      },
    )
      .chain(handleResponse((res) => res.status < 400))
      .bichain(
        // query failure
        handleRejectedResponse,
        // Success
        (res) =>
          Async.Resolved(
            ({
              ok: true,
              matches: pluck("_source", res.hits.hits),
            }),
          ),
      )
      .toPromise();
  }

  return Object.freeze({
    createIndex,
    deleteIndex,
    indexDoc,
    getDoc,
    updateDoc,
    removeDoc,
    bulk,
    query,
  });
}
