# WarpKV-Java 🚀

[![Java](https://img.shields.io/badge/Java-17-orange)](https://www.java.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](https://opensource.org/licenses/MIT)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen)]()

A lightweight **key-value store wrapper** in Java, combining **database-style storage** with a **server-style request interface**.  
Supports persistent storage, fast retrieval, and scalable operations using **WAL, SST files, and Bloom Filters**.

---

## Features ✨

- **Persistent Storage:** Data safely stored in SST files with WAL for crash recovery.  
- **In-Memory Efficiency:** Bloom Filters reduce unnecessary disk reads.  
- **Server Interface:** Minimal server API to handle programmatic requests.  
- **Lightweight & Fast:** Minimalistic, optimized for small-to-medium projects.

---

## Architecture 🏗️

+-------------------+ +----------------+
| Client Request | ---> | WrapKV Server |
+-------------------+ +----------------+
|
v
+-------------------+
| KV Store |
| (SST + WAL + |
| Bloom Filter) |
+-------------------+


- **WAL (Write-Ahead Log):** Guarantees durability in case of crashes.  
- **SST (Sorted String Table):** Efficient sorted storage for quick retrieval.  
- **Bloom Filter:** Probabilistic check to avoid unnecessary disk reads.

---

## Installation ⚡

1. Clone the repository:

```bash
git clone https://github.com/Neel654/wrapkv-java.git
cd warpkv-java
Build with Gradle:

Build with Gradle:
./gradlew build

Run the server:
./gradlew run
