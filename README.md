# go-disk-utils
My adventures in go; a small collection of disk utilities

## Overview

This started out as a teach-myself-go exercise but seems to be evolving into something useful so
I decided to share on github.

It's under heavy development and has had limited testing so no responsibility taken if
your cat spontaneously combusts when you run this software

## Installation

Assuming you have Go already installed and set up, simply run:
```
go get github.com/SeeSpotRun/go-disk-utils
go get github.com/SeeSpotRun/go-fibmap
```
You will also need to install some dependencies:
```
go get github.com/docopt/docopt-go

```

hddreader
---------
A package for governing file reads from an hdd to try to optimise performance.

sum
---
A utility which uses hddreader and walk to do (hopefully) high-speed checksum calculations
