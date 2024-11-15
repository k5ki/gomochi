
<h1 align="center">
  <br>
  Gomochi
  <br>
</h1>

<h4 align="center">A simple queue worker.</h4>

<p align="center">
  <a href="https://github.com/k5ki/gomochi/blob/main">
    <img alt="Check status" src="https://img.shields.io/github/checks-status/k5ki/gomochi/main" />
  </a>
  <a href="https://github.com/k5ki/gomochi/releases">
    <img alt="Latest release" src="https://img.shields.io/github/v/release/k5ki/gomochi?logo=starship&include_prerelease&sort=semver" />
  </a>
  <a href="https://github.com/k5ki/gomochi/blob/main/LICENSE">
     <img alt="License" src="https://img.shields.io/github/license/k5ki/gomochi" />
  </a>
  <a href="https://go.dev">
    <img alt="Go version" src="https://img.shields.io/github/go-mod/go-version/k5ki/gomochi?logo=go" />
  </a>
</p>

<p align="center">
  <a href="#overview">Overview</a> •
  <a href="#how-to-use">How To Use</a> •
  <a href="#license">License</a>
</p>

<div align="center"><img src="./docs/overview.svg" /></div>

## Overview

- Gomochi is a library  
  - Easily integrate it into your product
- Runs on a single host  
- Powered by AWS SQS (fifo)  
- Assigns workers per "MessageGroupId"  


## How To Use

```sh
go get github.com/k5ki/gomochi
```

See <a href="./example/main.go">example</a>.

## License

This software is released under the MIT License, [see LICENSE](https://github.com/k5ki/gomochi/blob/main/LICENSE).