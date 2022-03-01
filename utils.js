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
} = R;

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
  swap("_id", "__movedUnderscoreId63__"),
  identity,
);

export const toUnderscoreId = ifElse(
  has("__movedUnderscoreId63__"),
  swap("__movedUnderscoreId63__", "_id"),
  identity,
);
