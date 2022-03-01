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
import {
  esErrToHyperErr,
  handleHyperErr,
  moveUnderscoreId,
  toEsErr,
  toUnderscoreId,
} from "./utils.js";

const {
  compose,
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
  identity,
  ifElse,
  has,
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
   * Sometimes, elasticsearch automatically creates an index
   * we don't want that. So on some calls we check
   */
  const checkIndexExists = (index) =>
    asyncFetch(
      createIndexPath(config.origin, index),
      {
        headers,
        method: "GET",
      },
    )
      .chain(
        handleResponse((res) => res.status < 400),
      );

  /**
   * @param {IndexInfo}
   * @returns {Promise<Response>}
   */
  function createIndex({ index, mappings }) {
    let properties = mappings.fields.reduce(
      (a, f) => set(lensProp(f), { type: "text" }, a),
      {},
    );

    /**
     * _id is automatically mapped, and will produce an error if included,
     * so rename to id
     */
    properties = moveUnderscoreId(properties);

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
      .bimap(
        esErrToHyperErr({ subject: `index ${index}`, index: `index ${index}` }),
        identity,
      )
      .bichain(
        handleHyperErr,
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
      .bimap(
        esErrToHyperErr({ subject: `index ${index}`, index: `index ${index}` }),
        identity,
      )
      .bichain(
        handleHyperErr,
        always(Async.Resolved({ ok: true })),
      )
      .toPromise();
  }

  /**
   * @param {SearchDoc}
   * @returns {Promise<Response>}
   */
  function indexDoc({ index, key, doc }) {
    /**
     * From Elasticsearch:
     * Field [_id] is a metadata field and cannot be added inside a document.
     *
     * So we rename _id
     */
    doc = moveUnderscoreId(doc);

    // check index exists
    return checkIndexExists(index)
      .chain(() =>
        // Now actually index the document
        asyncFetch(
          indexDocPath(config.origin, index, key),
          {
            headers,
            method: "PUT",
            body: JSON.stringify(doc),
          },
        )
      )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bimap(
        esErrToHyperErr({
          subject: `document at key ${key}`,
          index: `index ${index}`,
        }),
        identity,
      )
      .bichain(
        handleHyperErr,
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
      .bimap(
        esErrToHyperErr({
          subject: `document at key ${key}`,
          index: `index ${index}`,
        }),
        toUnderscoreId,
      )
      .bichain(
        handleHyperErr,
        (res) => Async.Resolved({ ok: true, key, doc: res }),
      )
      .toPromise();
  }

  /**
   * @param {SearchDoc}
   * @returns {Promise<Response>}
   */
  function updateDoc({ index, key, doc }) {
    /**
     * From Elasticsearch:
     * Field [_id] is a metadata field and cannot be added inside a document.
     *
     * So we move _id to a field, and map it back when the document is pulled out
     */
    doc = moveUnderscoreId(doc);

    return checkIndexExists(index)
      .chain(() =>
        asyncFetch(
          updateDocPath(config.origin, index, key),
          {
            headers,
            method: "PUT",
            body: JSON.stringify(doc),
          },
        )
      )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bimap(
        esErrToHyperErr({
          subject: `document at key ${key}`,
          index: `index ${index}`,
        }),
        toUnderscoreId,
      )
      .bichain(
        handleHyperErr,
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
        // 404 not found, so just map to happy path
        handleResponse((res) => res.status < 400 || res.status === 404),
      )
      .bimap(
        esErrToHyperErr({
          subject: `document at key ${key}`,
          index: `index ${index}`,
        }),
        identity,
      )
      .bichain(
        handleHyperErr,
        always(Async.Resolved({ ok: true })),
      )
      .toPromise();
  }

  /**
   * @param {BulkIndex}
   * @returns {Promise<ResponseWithResults>}
   */
  function bulk({ index, docs }) {
    return checkIndexExists(index)
      .chain(() =>
        asyncFetch(
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
                ) => [
                  ...arr,
                  { index: { _index: index, _id: doc._id || doc.id } },
                  moveUnderscoreId(doc),
                ],
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
      )
      .chain(
        handleResponse((res) => res.status < 400),
      )
      .bimap(
        esErrToHyperErr({
          subject: `docs with ids ${pluck("id", docs).join(", ")}`,
          index: `index ${index}`,
        }),
        identity,
      )
      .bichain(
        handleHyperErr,
        /**
         * bulk response could be a mixture of success and hyper errors
         */
        (res) =>
          Async.Resolved({
            ok: true,
            results: map(
              ifElse(
                has("error"),
                (item) =>
                  compose(
                    esErrToHyperErr({
                      subject: `document at key ${item._id}`,
                      index: `index ${index}`,
                    }),
                    () =>
                      toEsErr({
                        ...item.error,
                        status: item.status,
                      }),
                  )(item),
                (item) => ({ ok: true, id: item._id }),
              ),
              pluck("index", res.items),
            ),
          }),
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
      .bimap(
        esErrToHyperErr({
          subject: `query against index ${index}`,
          index: `index ${index}`,
        }),
        identity,
      )
      .bichain(
        // query failure
        handleHyperErr,
        // Success
        (res) =>
          compose(
            (matches) => Async.Resolved({ ok: true, matches }),
            map(toUnderscoreId),
            pluck("_source"),
          )(res.hits.hits),
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
