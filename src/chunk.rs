use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub content: String,
    pub file_path: String,
    pub heading: String,
    pub frontmatter: Frontmatter,
}

#[derive(Debug, Clone, Default)]
pub struct Frontmatter {
    pub date: Option<String>,
    pub doc_type: Option<String>,
    pub tags: Option<Vec<String>>,
    pub project: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawFrontmatter {
    date: Option<serde_yaml::Value>,
    #[serde(rename = "type")]
    doc_type: Option<String>,
    tags: Option<Vec<String>>,
    project: Option<String>,
}

fn value_to_string(v: &serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::String(s) => s.clone(),
        _ => serde_yaml::to_string(v)
            .unwrap_or_default()
            .trim_end()
            .to_string(),
    }
}

/// Extract YAML frontmatter delimited by `---` lines.
/// Returns (parsed frontmatter, body after frontmatter).
/// On parse failure, returns default frontmatter and the original content.
// r[impl chunk.frontmatter.malformed]
pub fn parse_frontmatter(content: &str) -> (Frontmatter, &str) {
    let Some(rest) = content.strip_prefix("---\n") else {
        return (Frontmatter::default(), content);
    };
    let Some(end) = rest.find("\n---") else {
        return (Frontmatter::default(), content);
    };
    let (yaml_str, after) = rest.split_at(end);
    let body = after["\n---".len()..].trim_start_matches('\n');
    match serde_yaml::from_str::<RawFrontmatter>(yaml_str) {
        Ok(raw) => {
            let frontmatter = Frontmatter {
                date: raw.date.as_ref().map(value_to_string),
                doc_type: raw.doc_type,
                tags: raw.tags,
                project: raw.project,
            };
            (frontmatter, body)
        }
        Err(_) => (Frontmatter::default(), body),
    }
}

/// Split a markdown file into chunks by `##` and `###` headings.
/// Each chunk contains the heading prepended to the section text.
/// Files without headings become a single chunk.
pub fn chunk_file(file_path: &str, content: &str) -> Vec<Chunk> {
    let (frontmatter, body) = parse_frontmatter(content);
    let lines: Vec<&str> = body.lines().collect();
    let mut heading_indices: Vec<(usize, &str)> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        // r[impl chunk.heading-split]
        if line.starts_with("## ") || line.starts_with("### ") {
            heading_indices.push((i, line));
        }
    }

    // r[impl chunk.whole-file-fallback] r[impl chunk.meta.heading.fallback]
    if heading_indices.is_empty() {
        let filename = Path::new(file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(file_path);
        return vec![Chunk {
            content: body.to_string(),
            file_path: file_path.to_string(), // r[impl chunk.meta.file-path]
            heading: filename.to_string(),
            frontmatter,
        }];
    }

    let mut chunks = Vec::new();

    let first_heading_line = heading_indices[0].0;
    if first_heading_line > 0 {
        let preamble = lines[..first_heading_line].join("\n");
        let trimmed = preamble.trim();
        if !trimmed.is_empty() {
            let filename = Path::new(file_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(file_path);
            chunks.push(Chunk {
                content: trimmed.to_string(),
                file_path: file_path.to_string(),
                heading: filename.to_string(),
                frontmatter: frontmatter.clone(),
            });
        }
    }

    for (idx, &(line_idx, heading)) in heading_indices.iter().enumerate() {
        let end = heading_indices
            .get(idx + 1)
            .map(|(i, _)| *i)
            .unwrap_or(lines.len());
        let section_lines = &lines[line_idx..end];
        let content = section_lines.join("\n"); // r[impl chunk.content]
        chunks.push(Chunk {
            content,
            file_path: file_path.to_string(), // r[impl chunk.meta.file-path]
            heading: heading.to_string(),     // r[impl chunk.meta.heading]
            frontmatter: frontmatter.clone(), // r[impl chunk.meta.frontmatter]
        });
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    // r[verify chunk.heading-split]
    #[test]
    fn split_at_heading_boundaries() {
        let content = r#"## Section One
Content one.

### Subsection
Content two.

## Section Two
Content three."#;
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks.len(), 3);
        assert!(chunks[0].heading.contains("Section One"));
        assert!(chunks[1].heading.contains("Subsection"));
        assert!(chunks[2].heading.contains("Section Two"));
    }

    // r[verify chunk.whole-file-fallback]
    #[test]
    fn whole_file_when_no_headings() {
        let content = "Just some text\nwith no headings.";
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks.len(), 1);
    }

    // r[verify chunk.content]
    #[test]
    fn heading_prepended_to_content() {
        let content = "## My Heading\n\nBody text here.";
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].content.starts_with("## My Heading"));
        assert!(chunks[0].content.contains("Body text here."));
    }

    // r[verify chunk.meta.file-path]
    #[test]
    fn chunk_carries_file_path() {
        let content = "## Section\nContent";
        let chunks = chunk_file("path/to/file.md", content);
        assert_eq!(chunks[0].file_path, "path/to/file.md");
    }

    // r[verify chunk.meta.heading]
    #[test]
    fn chunk_carries_heading() {
        let content = "## Foo Bar\nContent";
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks[0].heading, "## Foo Bar");
    }

    // r[verify chunk.meta.heading.fallback]
    #[test]
    fn filename_as_heading_when_no_headings() {
        let content = "Plain content only.";
        let chunks = chunk_file("my-document.md", content);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].heading, "my-document.md");
    }

    // r[verify chunk.meta.frontmatter]
    #[test]
    fn chunk_carries_frontmatter() {
        let content = r#"---
date: 2024-01-15
type: note
tags: [a, b]
project: my-project
---
## Section
Content"#;
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].frontmatter.date.as_deref(), Some("2024-01-15"));
        assert_eq!(chunks[0].frontmatter.doc_type.as_deref(), Some("note"));
        assert_eq!(
            chunks[0].frontmatter.tags.as_ref().unwrap(),
            &["a".to_string(), "b".to_string()]
        );
        assert_eq!(chunks[0].frontmatter.project.as_deref(), Some("my-project"));
    }

    // r[verify chunk.frontmatter.malformed]
    #[test]
    fn malformed_frontmatter_omits_metadata() {
        let content = "---\nbad: yaml\n  indentation\n---\n## Section\nContent";
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].frontmatter.date, None);
        assert_eq!(chunks[0].frontmatter.doc_type, None);
        assert!(
            chunks[0].content.contains("## Section"),
            "file must still be chunked"
        );
    }

    #[test]
    fn preamble_content_before_first_heading_is_kept() {
        let content = "Introductory text here.\n\n## Section\nContent.";
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].heading, "doc.md");
        assert!(chunks[0].content.contains("Introductory text"));
        assert_eq!(chunks[1].heading, "## Section");
    }

    #[test]
    fn preamble_with_frontmatter() {
        let content = "---\ndate: 2024-01-01\n---\nPreamble text.\n\n## Section\nContent.";
        let chunks = chunk_file("doc.md", content);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].content.contains("Preamble text"));
        assert_eq!(chunks[0].frontmatter.date.as_deref(), Some("2024-01-01"));
    }

    #[test]
    fn parse_frontmatter_no_delimiters() {
        let content = "No frontmatter here.";
        let (fm, body) = parse_frontmatter(content);
        assert_eq!(fm.date, None);
        assert_eq!(body, "No frontmatter here.");
    }

    #[test]
    fn parse_frontmatter_valid() {
        let content = r#"---
date: 2024-01-01
type: note
---
Body after frontmatter."#;
        let (fm, body) = parse_frontmatter(content);
        assert_eq!(fm.date.as_deref(), Some("2024-01-01"));
        assert_eq!(fm.doc_type.as_deref(), Some("note"));
        assert_eq!(body.trim(), "Body after frontmatter.");
    }

    #[test]
    fn parse_frontmatter_malformed_returns_default() {
        let content = r#"---
bad: yaml
  indentation
---
Body"#;
        let (fm, body) = parse_frontmatter(content);
        assert_eq!(fm.date, None);
        assert_eq!(body.trim(), "Body");
    }

    #[test]
    fn parse_frontmatter_date_as_yaml_date() {
        let content = r#"---
date: 2024-03-15
---
Body"#;
        let (fm, _) = parse_frontmatter(content);
        assert_eq!(fm.date.as_deref(), Some("2024-03-15"));
    }
}
