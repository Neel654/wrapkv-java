# WarpKV-Java

A lightweight **key-value store wrapper** in Java, combining **database-style storage** with a **server-style request interface**.  
Supports persistent storage, efficient retrieval, and scalable operations using concepts like **WAL, SST files, and Bloom Filters**.

---

## Features

- **Persistent Storage:** Data is safely stored in SST files with WAL for crash recovery.  
- **In-Memory Efficiency:** Uses Bloom Filters to quickly check for key existence.  
- **Server Interface:** Provides a minimal server API to handle requests programmatically.  
- **Lightweight & Fast:** Designed to be minimal yet functional for small-to-medium projects.

---

## Architecture


