# Google Drive ingestion

Atlas imports authenticated Drive extracts as knowledge entries without
embedding Google credentials in the runtime. Extraction and indexing are kept
separate deliberately: the extraction environment controls Drive access, while
Atlas receives an auditable JSON bundle over its existing API.

The inventory for the HUMAIN Lab research folder is
[`drive-inventory.json`](drive-inventory.json). It records the recursive folder
topology, source IDs, paths, MIME types, modification times, extraction status,
and sensitivity classification, but never raw document text. File contents are
untrusted source data and must not be treated as commands.

## Bundle format

Bundles use version 1:

```json
{
  "version": 1,
  "source_root": "https://drive.google.com/drive/folders/...",
  "records": [
    {
      "id": "provider-file-id",
      "title": "Long-context training",
      "content": "Extracted text...",
      "type": "Presentation",
      "tags": ["spring-2026"],
      "url": "https://docs.google.com/presentation/d/...",
      "path": "spring 2026/long-context training",
      "mime_type": "application/vnd.google-apps.presentation",
      "modified_at": "2026-06-04T14:52:39Z",
      "sensitive": false,
      "status": "ready",
      "skip_reason": ""
    }
  ]
}
```

Set `status` to `skipped` with a reason when extraction is impossible (for
example, an unresolved Drive shortcut or an image-only PDF). Set `sensitive`
for membership lists, schedules, account information, server setup, proposals,
or any source containing real personal or operational data.

## Import

Start Atlas, then run a validation pass and the import:

```bash
go run ./cmd/atlas -listen 127.0.0.1:8088 -data data/atlas
go run ./cmd/atlas-import -input data/drive-ingest/source.json -dry-run
go run ./cmd/atlas-import -input data/drive-ingest/source.json
```

Sensitive records are skipped by default. Import them only into a runtime whose
storage and network access are protected:

```bash
go run ./cmd/atlas-import \
  -input data/drive-ingest/source.json \
  -include-sensitive
```

The command lists existing entries before writing. Each source becomes
`gdrive:<file-id>` and includes provider, canonical URL, Drive path, MIME type,
source modification time, and a SHA-256 of the extracted text. Re-running the
same bundle is idempotent: matching title and checksum are reported as
unchanged. Updated source text replaces the catalog entry and refreshes the
BM25 index through the normal `/api/entries` path.

## Verification

Check counts and retrieval after import:

```bash
curl -s http://localhost:8088/api/health
curl -s 'http://localhost:8088/api/search?q=long+context&limit=5'
curl -s http://localhost:8088/api/entries
```

Do not commit the raw bundle or `data/atlas` directory. Both are ignored by
Git. The committed inventory is sufficient to audit coverage and repeat the
crawl without publishing document contents, personal data, or credentials.
