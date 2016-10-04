# On-Disk Format

Sequins stores its data locally in the location you specify with
[local_store](../x-1-configuration-reference#localstore). This data is just a
cache, and is safe to delete when sequins isn't running.

Inside is a pidfile lock called `sequins.lock` and the `data` directory, which
actually holds the local data:

    /var/sequins
    ├── data
    │   └── <version>
    │       └── <version>
    │           ├── block-00000-c531151a-275f-47dc-b36a-784e71776081.spi
    │           ├── block-00000-c531151a-275f-47dc-b36a-784e71776081.spl
    │           ├── block-00001-52d71827-5480-4468-b0df-f3d6ad06b1ed.spi
    │           ├── block-00001-52d71827-5480-4468-b0df-f3d6ad06b1ed.spl
    │           ├── ...
    │           └── .manifest
    └── sequins.lock

The latter is organized into directories for each database and version, just
like in the source root. Inside each of those are:

 - Two files for each "block" - a log (`.spl`) and a hash (`.spi`) file - which
   together represent a single partition of the data, stored in the
   [Sparkey][sparkey] hashtable-on-disk format.

 - A `.manifest` file, which contains a list of the blocks present and some
   metadata for them. A definition for the manifest file can be found
   [here][manifest].

[sparkey]: https://github.com/spotify/sparkey
[manifest]: https://github.com/stripe/sequins/blob/c173493e4ffb9fa04cac3651291fdede1194f661/blocks/manifest.go
