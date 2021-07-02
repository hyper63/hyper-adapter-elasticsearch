<h1 align="center">hyper-adapter-elasticsearch</h1>
<p align="center">A Search port adapter that uses Elasticsearch to index documents for full text search in the <a href="https://hyper.io/">hyper</a>  service framework</p>
</p>
<p align="center">
  <a href="https://nest.land/package/hyper-adapter-elasticsearch"><img src="https://nest.land/badge.svg" alt="Nest Badge" /></a>
  <a href="https://github.com/hyper63/hyper-adapter-elasticsearch/actions/workflows/test.yml"><img src="https://github.com/hyper63/hyper-adapter-elasticsearch/actions/workflows/test.yml/badge.svg" alt="Test" /></a>
  <a href="https://github.com/hyper63/hyper-adapter-elasticsearch/tags/"><img src="https://img.shields.io/github/tag/hyper63/hyper-adapter-elasticsearch" alt="Current Version" /></a>
</p>

---

## Table of Contents

- [Getting Started](#getting-started)
- [Installation](#installation)
- [Features](#features)
- [Methods](#methods)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

```js
import { default as elasticsearch } from "https://x.nest.land/hyper-adapter-elasticsearch@0.1.2/mod.js";
const { fromNullable } = Either;

export default {
  app: opine,
  adapter: [
    {
      port: "search",
      plugins: [elasticsearch({ url: "http://localhost:9200" })],
    },
  ],
};
```

## Installation

This is a Deno module available to import from
[nest.land](https://nest.land/package/hyper-adapter-elasticsearch)

deps.js

```js
export { default as elasticsearch } from "https://x.nest.land/hyper-adapter-elasticsearch@0.1.2/mod.js";
```

## Features

- create an index in Elasticsearch
- delete an index in Elasticsearch
- index a document using Elasticsearch
- retrieving an indexed document from Elasticsearch index
- update an indexed document in Elasticsearch index
- remove an indexed document from Elasticsearch index
- bulk operation to index multiple docs using Elasticsearch (uses
  Elasticsearches
  [bulk api](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk))
- query an Elasticsearch index

## Methods

This adapter fully implements the Search port and can be used as the
[hyper Search service](https://docs.hyper.io/search-api) adapter

See the full port [here](https://nest.land/package/hyper-port-search)

## Contributing

Contributions are welcome! See the hyper
[contribution guide](https://docs.hyper.io/contributing-to-hyper)

## License

Apache-2.0
