use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use std::sync::Mutex;

pub trait Embedder: Send + Sync {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>>;
    fn model_name(&self) -> &str;
    fn dimension(&self) -> usize;
}

// r[impl index.embed.in-process]
pub struct FastEmbedder {
    model: Mutex<TextEmbedding>,
    model_name: String,
    dimension: usize,
}

impl FastEmbedder {
    pub fn new(model: EmbeddingModel) -> anyhow::Result<Self> {
        let model_name = model.to_string();
        let mut embedding = TextEmbedding::try_new(InitOptions::new(model))?;
        let dimension = embedding
            .embed(&["dimension probe".to_string()], None)?
            .first()
            .map(|v| v.len())
            .ok_or_else(|| anyhow::anyhow!("failed to determine embedding dimension"))?;
        Ok(Self {
            model: Mutex::new(embedding),
            model_name,
            dimension,
        })
    }

    pub fn default_model() -> anyhow::Result<Self> {
        Self::new(EmbeddingModel::BGESmallENV15)
    }
}

impl Embedder for FastEmbedder {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        let mut model = self
            .model
            .lock()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {}", e))?;
        let embeddings = model.embed(texts, None)?;
        Ok(embeddings)
    }

    fn model_name(&self) -> &str {
        &self.model_name
    }

    fn dimension(&self) -> usize {
        self.dimension
    }
}

#[cfg(test)]
pub struct MockEmbedder {
    dimension: usize,
    model_name: String,
}

#[cfg(test)]
impl MockEmbedder {
    pub fn new(dimension: usize, model_name: impl Into<String>) -> Self {
        Self {
            dimension,
            model_name: model_name.into(),
        }
    }
}

#[cfg(test)]
impl Embedder for MockEmbedder {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|_| vec![0.1; self.dimension]).collect())
    }

    fn model_name(&self) -> &str {
        &self.model_name
    }

    fn dimension(&self) -> usize {
        self.dimension
    }
}

/// Produces distinct vectors by hashing the input text, so cosine distance
/// varies between different inputs. Useful for testing search ranking.
#[cfg(test)]
pub struct HashEmbedder {
    dimension: usize,
    model_name: String,
}

#[cfg(test)]
impl HashEmbedder {
    pub fn new(dimension: usize, model_name: impl Into<String>) -> Self {
        Self {
            dimension,
            model_name: model_name.into(),
        }
    }

    fn text_to_vector(&self, text: &str) -> Vec<f32> {
        use std::hash::{Hash, Hasher};
        let mut vec = Vec::with_capacity(self.dimension);
        for i in 0..self.dimension {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            text.hash(&mut hasher);
            i.hash(&mut hasher);
            let h = hasher.finish();
            vec.push((h as f32) / (u64::MAX as f32));
        }
        vec
    }
}

#[cfg(test)]
impl Embedder for HashEmbedder {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| self.text_to_vector(t)).collect())
    }

    fn model_name(&self) -> &str {
        &self.model_name
    }

    fn dimension(&self) -> usize {
        self.dimension
    }
}

/// Fails on batch (len > 1) but succeeds on single-item calls.
/// Exercises the batch-fail fallback path in embed_chunks.
#[cfg(test)]
pub struct BatchFailEmbedder {
    dimension: usize,
    model_name: String,
}

#[cfg(test)]
impl BatchFailEmbedder {
    pub fn new(dimension: usize, model_name: impl Into<String>) -> Self {
        Self {
            dimension,
            model_name: model_name.into(),
        }
    }
}

#[cfg(test)]
impl Embedder for BatchFailEmbedder {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        if texts.len() > 1 {
            anyhow::bail!("batch embedding not supported");
        }
        Ok(texts.iter().map(|_| vec![0.1; self.dimension]).collect())
    }

    fn model_name(&self) -> &str {
        &self.model_name
    }

    fn dimension(&self) -> usize {
        self.dimension
    }
}
