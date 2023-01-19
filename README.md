# Atylar

Atylar is an opinionated file storage system with version history.
It uses a flat directory structure (no subdirectories). To start, initialize a new `Store` using the function `New()`,
supplying the store's root directory path as the argument. All functions which may be used to modify the files
automatically copy the current file to the `.history` directory in the current store. Historic versions are marked
with an @ sign and the version number after the file name. The numbers are designated based on the generation,
an always-increasing counter characteristic for the store.