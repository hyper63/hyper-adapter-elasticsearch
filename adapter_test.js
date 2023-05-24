import { assert, assertEquals, assertObjectMatch, spy } from './dev_deps.js'

import createAdapter from './adapter.js'
import { asyncFetch, createHeaders, handleResponse } from './async-fetch.js'
import {
  bulkPath,
  createIndexPath,
  deleteIndexPath,
  getDocPath,
  indexDocPath,
  queryPath,
  removeDocPath,
  updateDocPath,
} from './paths.js'
import { moveUnderscoreId, underscoreIdAlias } from './utils.js'

const headers = createHeaders('admin', 'password')

const ES = 'http://localhost:9200'
const INDEX = 'movies'

const DOC1 = {
  title: 'The Great Gatsby',
  id: 'tgg',
  year: 2012,
  rating: 4,
}

const DOC2 = {
  title: 'The Foo Gatsby',
  id: 'tfg',
  year: 2012,
  rating: 6,
}

const responses = []
const queueResponse = (status, body) => {
  responses.unshift({ status, body })
}

const fetch = spy(() => {
  // default to 200 { ok: true }
  const { status, body } = responses.pop() ||
    { status: 200, body: { ok: true } }

  return Promise.resolve({ json: () => Promise.resolve(body), status })
})

const cleanup = () => {
  fetch.calls.splice(0, fetch.calls.length)
  responses.splice(0, responses.length)
}

const adapter = createAdapter({
  config: { origin: ES },
  asyncFetch: asyncFetch(fetch),
  headers,
  handleResponse,
})

Deno.test('remove index', async () => {
  // remove index
  queueResponse(200, { ok: true })

  const result = await adapter.deleteIndex(INDEX)

  assertObjectMatch(fetch.calls.pop(), {
    args: [deleteIndexPath(ES, INDEX), {
      method: 'DELETE',
      headers,
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('remove index - not found', async () => {
  // remove index
  queueResponse(404, {
    error: {
      type: 'index_not_found_exception',
      reason: 'foo',
    },
  })

  const result = await adapter.deleteIndex(INDEX)

  assertEquals(result.ok, false)
  assertEquals(result.status, 404)
  assertEquals(result.msg, `index ${INDEX} not found`)

  cleanup()
})

Deno.test('create index', async () => {
  // create index
  queueResponse(201, { ok: true })

  const result = await adapter.createIndex({
    index: INDEX,
    mappings: { fields: ['title'] },
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [createIndexPath(ES, INDEX), {
      method: 'PUT',
      headers,
      body: '{"mappings":{"properties":{"title":{"type":"text"}}}}',
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('create index - maps _id', async () => {
  // create index
  queueResponse(201, { ok: true })

  const result = await adapter.createIndex({
    index: INDEX,
    mappings: { fields: ['_id', 'title'] },
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [createIndexPath(ES, INDEX), {
      method: 'PUT',
      headers,
      body:
        `{"mappings":{"properties":{"title":{"type":"text"},"${underscoreIdAlias}":{"type":"text"}}}}`,
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('create index - error', async () => {
  // create index
  queueResponse(404, {
    error: {
      type: 'mapper_parsing_exception',
      reason: 'foo',
    },
  })

  const result = await adapter.createIndex({
    index: INDEX,
    mappings: { fields: ['title'] },
  })

  assertEquals(result.ok, false)
  assertEquals(result.status, 422)
  assertEquals(result.msg, `failed to parse mapping for index ${INDEX}: foo`)

  cleanup()
})

Deno.test('index document', async () => {
  // check for existing index
  queueResponse(200)
  // check for existing doc
  queueResponse(404)
  // index doc
  queueResponse(200, { ok: true })

  const result = await adapter.indexDoc({
    index: INDEX,
    key: DOC1.id,
    doc: DOC1,
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [indexDocPath(ES, INDEX, DOC1.id), {
      method: 'PUT',
      headers,
      body: JSON.stringify(DOC1),
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('index document - conflict', async () => {
  // check for existing index
  queueResponse(200)
  // check for existing doc
  queueResponse(200)

  const result = await adapter.indexDoc({
    index: INDEX,
    key: DOC1.id,
    doc: DOC1,
  })

  assertObjectMatch(result, {
    ok: false,
    status: 409,
    msg: 'document conflict',
  })

  cleanup()
})

Deno.test('index document - error', async () => {
  // index doc
  queueResponse(404, {
    error: {
      type: 'index_not_found_exception',
      reason: 'foo',
    },
  })

  const result = await adapter.indexDoc({
    index: INDEX,
    key: DOC1.id,
    doc: DOC1,
  })

  assertEquals(result.ok, false)
  assertEquals(result.status, 404)
  assertEquals(result.msg, `index ${INDEX} not found`)

  cleanup()
})

Deno.test('index document - maps _id', async () => {
  // check for existing index
  queueResponse(200)
  // check for existing doc
  queueResponse(404)
  // index doc
  queueResponse(200, { ok: true })

  const withUnderscoreId = {
    ...DOC1,
    _id: DOC1.id,
  }

  const result = await adapter.indexDoc({
    index: INDEX,
    key: DOC1.id,
    doc: withUnderscoreId,
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [indexDocPath(ES, INDEX, DOC1.id), {
      method: 'PUT',
      headers,
      body: JSON.stringify(moveUnderscoreId(withUnderscoreId)),
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('get document', async () => {
  // get doc
  queueResponse(200, DOC1)

  const result = await adapter.getDoc({
    index: INDEX,
    key: DOC1.id,
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [getDocPath(ES, INDEX, DOC1.id), {
      method: 'GET',
      headers,
    }],
  })

  assertEquals(result.doc.title, DOC1.title)
  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('get document - error doc not found', async () => {
  // get doc
  queueResponse(404, {
    error: {
      type: 'resource_not_found_exception',
      reason: 'foo',
    },
  })

  const result = await adapter.getDoc({
    index: INDEX,
    key: DOC1.id,
  })

  assertEquals(result.ok, false)
  assertEquals(result.status, 404)
  assertEquals(result.msg, `document at key ${DOC1.id} not found`)

  cleanup()
})

Deno.test('get document - error index not found', async () => {
  // get doc
  queueResponse(404, {
    error: {
      type: 'index_not_found_exception',
      reason: 'foo',
    },
  })

  const result = await adapter.getDoc({
    index: INDEX,
    key: DOC1.id,
  })

  assertEquals(result.ok, false)
  assertEquals(result.status, 404)
  assertEquals(result.msg, `index ${INDEX} not found`)

  cleanup()
})

Deno.test('update document', async () => {
  // update doc
  queueResponse(201, { ok: true })

  const result = await adapter.updateDoc({
    index: INDEX,
    key: DOC1.id,
    doc: {
      ...DOC1,
      rating: 6,
    },
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [updateDocPath(ES, INDEX, DOC1.id), {
      method: 'PUT',
      headers,
      body: JSON.stringify({
        ...DOC1,
        rating: 6,
      }),
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

// update document sad paths covered by other tests

Deno.test('delete document', async () => {
  // remove doc
  queueResponse(201, { ok: true })

  const result = await adapter.removeDoc({
    index: INDEX,
    key: DOC1.id,
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [removeDocPath(ES, INDEX, DOC1.id), {
      method: 'DELETE',
      headers,
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('delete document - 404 passthrough', async () => {
  // remove doc
  queueResponse(404, { ok: true })

  const result = await adapter.removeDoc({
    index: INDEX,
    key: 'not_found',
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [removeDocPath(ES, INDEX, 'not_found'), {
      method: 'DELETE',
      headers,
    }],
  })

  assertEquals(result.ok, true)

  cleanup()
})

Deno.test('bulk', async () => {
  // check for index
  queueResponse(200)
  // bulk operation
  queueResponse(200, {
    items: [
      { index: { _id: DOC1.id, ...DOC1 } },
      { index: { _id: DOC2.id, ...DOC2 } },
    ],
  })

  const result = await adapter.bulk({
    index: INDEX,
    docs: [
      DOC1,
      DOC2,
    ],
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [bulkPath(ES), {
      method: 'POST',
      headers,
      // TODO: Tyler. Assert body here eventually
    }],
  })

  assertEquals(result.ok, true)
  assert(result.results)

  cleanup()
})

Deno.test('bulk - maps all _ids and operations', async () => {
  // check for index
  queueResponse(200)
  // bulk operation
  queueResponse(200, {
    items: [
      { index: { _id: DOC1.id, ...DOC1 } },
      { index: { _id: DOC2.id, ...DOC2 } },
    ],
  })

  const doc1WithUnderscoreId = { ...DOC1, _id: DOC1.id }
  await adapter.bulk({
    index: INDEX,
    docs: [
      doc1WithUnderscoreId,
      DOC2,
    ],
  })

  const call = fetch.calls.pop()
  assertObjectMatch(call, {
    args: [bulkPath(ES), {
      method: 'POST',
      headers,
    }],
  })

  let { args: [, { body }] } = call

  body = body.split('\n')
  console.log(body)
  body = body.slice(0, -1) // remove \n from end of list
  const [index1, first, index2] = body

  assertEquals(doc1WithUnderscoreId._id, JSON.parse(index1).index._id)
  assertEquals(DOC2.id, JSON.parse(index2).index._id)

  assertEquals(first, JSON.stringify(moveUnderscoreId(doc1WithUnderscoreId)))

  cleanup()
})

Deno.test('bulk - entire request error', async () => {
  // bulk operation
  queueResponse(404, {
    error: {
      type: 'index_not_found_exception',
      reason: 'foo',
    },
  })

  const result = await adapter.bulk({
    index: INDEX,
    docs: [
      DOC1,
      DOC2,
    ],
  })

  assertEquals(result.ok, false)
  assertEquals(result.status, 404)
  assertEquals(result.msg, `index ${INDEX} not found`)

  cleanup()
})

Deno.test('bulk - check each doc has an id or _id', async () => {
  // check for index
  queueResponse(200)

  const result = await adapter.bulk({
    index: INDEX,
    docs: [
      { _id: DOC1.id, ...DOC1 },
      DOC2,
      { no_id: 'foo', fizz: 'buzz' },
    ],
  })

  assertEquals(result.ok, false)
  assertEquals(result.status, 422)
  assertEquals(result.msg, 'Each document must have an id or _id field')

  cleanup()
})

Deno.test('bulk - with errors', async () => {
  // check for index
  queueResponse(200)
  // bulk operation
  queueResponse(200, {
    items: [
      { index: { _id: DOC1.id, ...DOC1 } },
      {
        index: {
          _id: DOC2.id,
          ...DOC2,
          status: 400,
          error: { type: 'resource_already_exists_exception', reason: 'foo' },
        },
      },
    ],
  })

  const result = await adapter.bulk({
    index: INDEX,
    docs: [
      DOC1,
      DOC2,
    ],
  })

  assertObjectMatch(fetch.calls.pop(), {
    args: [bulkPath(ES), {
      method: 'POST',
      headers,
      // TODO: Tyler. Assert body here eventually
    }],
  })

  assertEquals(result.ok, true)
  assert(result.results)
  const [success, err] = result.results

  assert(success.ok)
  assert(success.id)

  assertEquals(err.status, 409)
  assertEquals(err.msg, `document at key ${DOC2.id} already exists`)
  assertEquals(err.ok, false)

  cleanup()
})

Deno.test('query', async () => {
  // query docs
  queueResponse(200, {
    hits: {
      hits: [
        { _source: DOC1 },
        { _source: moveUnderscoreId({ ...DOC2, _id: DOC2.id }) },
      ],
    },
  })

  const result = await adapter.query({
    index: 'movies',
    q: {
      query: 'gatsby',
      fields: ['title', '_id'],
      filter: {
        rating: 4,
        _id: 'id-1',
      },
    },
  })

  const call = fetch.calls.pop()
  assertObjectMatch(call, {
    args: [queryPath(ES, INDEX), {
      method: 'POST',
      headers,
    }],
  })

  let { args: [, { body }] } = call
  body = JSON.parse(body)

  assertObjectMatch(body, {
    query: {
      bool: {
        must: {
          multi_match: {
            query: 'gatsby',
            fuzziness: 'AUTO',
            fields: ['title', underscoreIdAlias],
          },
        },
        filter: [
          { term: { rating: 4 } },
          { term: { [underscoreIdAlias]: 'id-1' } },
        ],
      },
    },
  })

  assertEquals(result.ok, true)
  assert(result.matches)
  assertEquals(result.matches.length, 2)

  assertEquals(result.matches[0].id, DOC1.id)
  // maps _id back
  assertEquals(result.matches[1]._id, DOC2.id)

  cleanup()
})

Deno.test('query - error', async () => {
  // query docs
  queueResponse(404, {
    error: {
      type: 'index_not_found_exception',
      reason: 'foo',
    },
  })

  const result = await adapter.query({
    index: 'movies',
    q: {
      query: 'gatsby',
      fields: ['title'],
      filter: {
        rating: 4,
      },
    },
  })

  assertEquals(result.ok, false)
  assertEquals(result.status, 404)
  assertEquals(result.msg, `index ${INDEX} not found`)

  cleanup()
})
