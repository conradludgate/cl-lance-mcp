# Chunking

Markdown files are split into chunks for embedding. Chunking is heading-based,
preserving document structure. Each chunk carries metadata extracted from the
source file's frontmatter.

## Splitting

r[chunk.heading-split]
Files MUST be split into chunks at `##` and `###` heading boundaries.

r[chunk.whole-file-fallback]
Files without headings MUST be treated as a single chunk.

## Content

r[chunk.content]
Each chunk MUST contain the heading prepended to the section text. The heading
carries semantic signal that improves embedding quality.

## Metadata

r[chunk.meta.file-path]
Each chunk MUST carry the source file path in its metadata.

r[chunk.meta.heading]
Each chunk MUST carry the section heading text in its metadata.

r[chunk.meta.heading.fallback]
For whole-file chunks (no headings), the heading field MUST be set to the
filename.

r[chunk.meta.frontmatter]
Each chunk MUST carry frontmatter fields (`date`, `type`, `tags`, `project`)
in its metadata when present in the source file.

## Error Handling

r[chunk.frontmatter.malformed]
If frontmatter is present but malformed, the file MUST still be chunked with
frontmatter metadata fields omitted.
