import { crocks, R } from "./deps.js";

const { Async } = crocks;
const {
  __,
  assoc,
  dissoc,
  isEmpty,
  ifElse,
  defaultTo,
  propEq,
  cond,
  is,
  identity,
  T,
  complement,
  isNil,
  compose,
  has,
  allPass,
  anyPass,
  filter,
  evolve,
  applyTo,
  propOr,
  always,
  path,
  flip,
  join,
  reduce,
  map,
  concat,
  set,
  lensProp,
  toPairs,
} = R;

export const underscoreIdAlias = "__movedUnderscoreId63__";

const isDefined = complement(isNil);
const isEmptyObject = allPass([
  complement(is(Array)), // not an array
  is(Object),
  isEmpty,
]);
const rejectNil = filter(isDefined);

/**
 * Constructs a hyper-esque error
 *
 * @typedef {Object} HyperErrArgs
 * @property {string} msg
 * @property {string?} status
 *
 * @typedef {Object} NotOk
 * @property {false} ok
 *
 * @param {(HyperErrArgs | string)} argsOrMsg
 * @returns {NotOk & HyperErrArgs} - the hyper-esque error
 */
export const HyperErr = (argsOrMsg) =>
  compose(
    ({ ok, msg, status }) => rejectNil({ ok, msg, status }), // pick and filter nil
    assoc("ok", false),
    cond([
      // string
      [is(String), assoc("msg", __, {})],
      // { msg?, status? }
      [
        anyPass([
          isEmptyObject,
          has("msg"),
          has("status"),
        ]),
        identity,
      ],
      // Fallthrough to error
      [T, () => {
        throw new Error(
          "HyperErr args must be a string or an object with msg or status",
        );
      }],
    ]),
    defaultTo({}),
  )(argsOrMsg);

export const isHyperErr = allPass([
  propEq("ok", false),
  /**
   * should not have an _id.
   * Otherwise it's a document ie data.retrieveDocument
   * or cache.getDoc
   */
  complement(has("_id")),
]);

export const handleHyperErr = ifElse(
  isHyperErr,
  Async.Resolved,
  Async.Rejected,
);

export const toEsErr = (err, fallbackStatus = 500) => ({
  err, // backreference
  /**
   * used caused_by
   * fallback to top-lvl reason
   * fallback to generic error message
   */
  reason: compose(
    defaultTo(propOr("an error occurred", "reason", err)),
    path(["caused_by", "reason"]),
  )(err),
  type: propOr("unknown", "type", err),
  /**
   * use body status
   * fallback to response status
   */
  status: propOr(fallbackStatus, "status", err),
});

/**
 * Generate string templates
 */
const template = (strings, ...keys) =>
  (dict) => {
    const result = [strings[0]];
    keys.forEach((key, i) => {
      result.push(dict[key], strings[i + 1]);
    });
    return result.join("");
  };

export const esErrToHyperErr = (context) =>
  compose(
    HyperErr,
    ({ status, type, reason }) =>
      evolve(
        { msg: applyTo({ reason, ...context }) }, // populate the msg template
        /**
         * Set status and msg template
         */
        propOr(
          { status, msg: always(reason) },
          type,
          {
            resource_already_exists_exception: {
              status: 409,
              msg: template`${"subject"} already exists`,
            },
            mapper_parsing_exception: {
              status: 422,
              msg: template
                `failed to parse mapping for ${"subject"}: ${"reason"}`,
            },
            index_not_found_exception: {
              status: 404,
              msg: template`${"index"} not found`,
            },
            resource_not_found_exception: {
              status: 404,
              msg: template`${"subject"} not found`,
            },
            not_found: {
              status: 404,
              msg: template`${"subject"} not found`,
            },
          },
        ),
      ),
  );

const swap = (old, cur) =>
  compose(
    dissoc(old),
    (o) => assoc(cur, o[old], o),
  );

export const moveUnderscoreId = ifElse(
  has("_id"),
  swap("_id", underscoreIdAlias),
  identity,
);

export const toUnderscoreId = ifElse(
  has(underscoreIdAlias),
  swap(underscoreIdAlias, "_id"),
  identity,
);

/**
 * Create an elasticsearch _bulk index payload
 *
 * See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk-api-example
 *
 * @param {string} index
 * @param {object[]} docs
 * @returns {string} - the bulk payload to send to elasticsearch
 */
export const bulkToEsBulk = (index, docs) =>
  compose(
    // Bulk payload must end with a newline
    flip(concat)("\n"),
    join("\n"),
    // stringify each object in arr
    map(JSON.stringify.bind(JSON)),
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
  )(docs);

/**
 * @param {mappings} - hyper index mappings { fields }
 * @returns {object} -
 */
export const mappingsToEsMappings = compose(
  (properties) => ({
    mappings: { properties },
  }),
  /**
   * _id is automatically mapped, and will produce an error if included,
   * so rename id
   */
  moveUnderscoreId,
  (mappings) =>
    mappings.fields.reduce(
      (a, f) => set(lensProp(f), { type: "text" }, a),
      {},
    ),
  defaultTo({ fields: [] }),
);

export const queryToEsQuery = ({ query, fields, filter }) => ({
  query: {
    bool: {
      must: {
        multi_match: {
          query,
          // See https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html#query-dsl-match-query-fuzziness
          fuzziness: "AUTO",
          // map _id => underscoreIdAlias
          fields: fields
            ? fields.map((field) => field === "_id" ? underscoreIdAlias : field)
            : undefined,
        },
      },
      filter: toPairs(filter).map(
        // map _id => underscoreIdAlias
        ([key, value]) =>
          key === "_id"
            ? ({ term: { [underscoreIdAlias]: value } })
            : ({ term: { [key]: value } }),
      ),
    },
  },
});
