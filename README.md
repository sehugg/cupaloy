# Cupaloy

A long-term digital data preservation tool for managing file replication, tracking changes, and analyzing data formats across multiple storage locations.

## Overview

Cupaloy aids humans in the long-term preservation of their digital data by targeting three key areas:

### Replication

Since most forms of storage are subject to unforeseen events leading to data loss, it's best to have more than one copy stored in more than one location. Cupaloy scans your files in each location and gives you a simple readout of how many files are up-to-date across all locations. You can configure thresholds to warn you when files are not sufficiently replicated, or if too much time has passed between scans.

### Change Tracking

Since all modifications to your file collections may not necessarily be intentional, Cupaloy helps you understand when files in your collections have changed. It will give you a simple summary of what has changed, and asks you to confirm these changes. For example, you might configure a collection so that additions are automatically accepted, but deletions and modifications need to be confirmed.

### Data Format Analysis

Data formats wax and wane over time, and sometimes are more complicated than just a file extension—video formats, for example, are notoriously complicated. Cupaloy uses helper applications to analyze and verify the format of files, and to peek inside of archives. The goal is to predict when file formats start to become obsolete, and notify you so you can migrate to a different format before it becomes difficult to do so.

## Design Philosophy

Cupaloy is designed for end users, not professional curators. We try to use terminology the average user will understand, and sensible defaults wherever possible. Set-it-and-forget-it is the goal.

## Quick Start

```bash
# Initialize a collection
python main.py --name <name> init <directory>

# Scan a collection
python main.py scan <collection>

# View status of all collections
python main.py status

# List volumes and collections
python main.py list
```

## Example Output

```
3 collections.
1 (Huggs Archive) is online, replicated twice locally and once remotely.
1 (PuzzlingPlans) is online, not replicated.
1 (AncientStuff) is offline, last seen 30 days ago.

NAME            Replicas    Where?          Coverage    Last check
---
Huggs Archive   2           local/remote    100%        2 days ago
```

## Core Concepts

**Collection**: A set of files found at one or more locations, meant to be backed up/mirrored. A collection has a UUID (either randomly assigned, directly assigned by user, or derived from a location) and a name.

**Location**: A directory on a filesystem or a URL which refers to a set of files. Locations can be local (`file://` URLs) or remote (`s3://` URLs for AWS S3). Paths are relative to the mounted volume or network location root.

**Snapshot**: A scan of a collection at a given location. Contains details about the scan results, scan duration, and scan options. It also records the last known names of the collection and location, and the hostname performing the scan.

**Volume**: A physical storage device identified by UUID, tracked across scans to detect when drives are mounted/unmounted.

## Usage

### Initialize a Collection

```bash
python main.py --name <name> [--uuid <uuid>] init [directory]
```

### Scan Collections

```bash
# Scan all collections
python main.py scan

# Scan specific collections
python main.py scan [collection...]

# Force rescan with additional options
python main.py --force --rescan --omithash scan [collection...]

# Scan and process archive files
python main.py --archives scan [collection...]
```

### View Status

```bash
# View collection status
python main.py status [collection...]

# Include archive contents in status
python main.py --archives status [collection...]
```

### List Volumes and Collections

```bash
# List all collections and volumes
python main.py list

# List for specific host
python main.py --host <name> list
```

### Find Duplicates

```bash
python main.py [--archives] dups
```

### Rename or Retire Collections

```bash
# Rename a collection
python main.py --name <name> [--uuid <uuid>] rename [collection]

# Retire a collection
python main.py retire [collection]
```

## Supported Storage Backends

- **Local/Network filesystems**: via `file://` URLs
- **AWS S3**: via `s3://` URLs (including Glacier detection)

## Archive Support

When scanning with the `--archives` flag, Cupaloy can read inside archive files:

- **Tar formats**: `.tar`, `.tgz`, `.tbz2`
- **Zip files**: `.zip`
- **Other archives**: `.iso`, `.pax`, `.cpio`, `.xar`, `.rar` (via libarchive)

## Testing

```bash
make test
```

Or alternatively:

```bash
python ./common.py
python -m unittest discover -v
```

## File Locations

- **Global host database**: `~/.cupaloy/hosts/<hostname>.db`
- **Collection databases**: `~/.cupaloy/collections/<collection-uuid>/<url-encoded-location>.db`
- **Collection metadata**: `<collection-root>/.cupaloy/collection.json`

The `CUPALOY_HOME` environment variable can override the default `~/.cupaloy` location.

## Features

- SHA512 checksums for file verification
- Volume tracking across scans
- Change history for all files
- Custom include/exclude patterns
- Support for virtual files inside archives
- Replication coverage analysis
- Time-based scan warnings
- Multiple storage location support

## Requirements

- Python 3.x
- SQLite3
- boto3 (for S3 support)
- libarchive (for extended archive format support)

## License

See LICENSE file for details.
