use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
struct Document {
    path: String,
    length: usize,
    size: u64,
    modified_unix: u64,
}

#[derive(Debug, Clone)]
struct Posting {
    doc_id: usize,
    tf: usize,
}

#[derive(Debug, Clone)]
struct Index {
    root: String,
    created_at_unix: u64,
    docs: Vec<Document>,
    inverted: HashMap<String, Vec<Posting>>,
    avg_doc_len: f64,
}

#[derive(Debug, Default, Clone, Copy)]
struct BuildStats {
    scanned_files: usize,
    indexed_files: usize,
    skipped_files: usize,
    reused_files: usize,
}

#[derive(Debug, Clone)]
struct FileEntry {
    abs: PathBuf,
    rel: String,
    size: u64,
    modified_unix: u64,
}

enum WorkerMsg {
    Indexed {
        rel_path: String,
        size: u64,
        modified_unix: u64,
        token_count: usize,
        tf_map: HashMap<String, usize>,
    },
    Skipped,
}

#[derive(Debug, Clone)]
struct IndexConfig {
    root: String,
    out: String,
    workers: usize,
    max_file_size: u64,
    ext_filter: HashSet<String>,
    incremental: bool,
    in_index: String,
}

#[derive(Debug, Clone)]
struct QueryConfig {
    index_path: String,
    query: String,
    top_k: usize,
    k1: f64,
    b: f64,
}

#[derive(Debug, Clone)]
struct ProfileConfig {
    root: String,
    workers: usize,
    max_file_size: u64,
    ext_filter: HashSet<String>,
    queries: Vec<String>,
    top_k: usize,
    k1: f64,
    b: f64,
}

#[derive(Debug)]
struct ResultItem {
    doc_id: usize,
    score: f64,
}

#[derive(Debug)]
struct ParsedQuery {
    mode: QueryMode,
}

#[derive(Debug)]
enum QueryMode {
    OrTerms(Vec<String>),
    OrOfAndClauses(Vec<Vec<String>>),
}

fn main() {
    let mut args = env::args().skip(1);
    let Some(cmd) = args.next() else {
        print_usage();
        std::process::exit(1);
    };

    let rest: Vec<String> = args.collect();
    let result = match cmd.as_str() {
        "index" => parse_index_config(&rest).and_then(run_index),
        "query" => parse_query_config(&rest).and_then(run_query),
        "profile" => parse_profile_config(&rest).and_then(run_profile),
        _ => {
            print_usage();
            Err(format!("unknown command: {cmd}"))
        }
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn print_usage() {
    eprintln!("Local Code Search Engine (Rust)");
    eprintln!("Usage:");
    eprintln!(
        "  codesearch index --root . --out index.idx --workers 8 --max-file 1048576 --ext rs,go,md"
    );
    eprintln!("  codesearch index --root . --out index.idx --incremental --in index.idx");
    eprintln!(
        "  codesearch query --index index.idx --q \"tokenize AND bm25 OR goroutine\" --top 10"
    );
    eprintln!("  codesearch profile --root . --ext rs --queries \"bm25;index AND query\"");
}

fn parse_index_config(args: &[String]) -> Result<IndexConfig, String> {
    let root = arg_or_default(args, "--root", ".")?;
    let out = arg_or_default(args, "--out", "index.idx")?;
    let workers = arg_or_default(args, "--workers", "0")?
        .parse::<usize>()
        .map_err(|_| "invalid --workers".to_string())?;
    let max_file_size = arg_or_default(args, "--max-file", "1048576")?
        .parse::<u64>()
        .map_err(|_| "invalid --max-file".to_string())?;
    let ext_raw = arg_or_default(args, "--ext", "")?;
    let incremental = has_flag(args, "--incremental");
    let in_index = arg_or_default(args, "--in", &out)?;

    let mut ext_filter = HashSet::new();
    if !ext_raw.trim().is_empty() {
        for ext in ext_raw.split(',') {
            let e = normalize_ext(ext);
            if !e.is_empty() {
                ext_filter.insert(e);
            }
        }
    }

    let worker_count = if workers == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        workers
    };

    Ok(IndexConfig {
        root,
        out,
        workers: worker_count.max(1),
        max_file_size,
        ext_filter,
        incremental,
        in_index,
    })
}

fn parse_query_config(args: &[String]) -> Result<QueryConfig, String> {
    let index_path = arg_or_default(args, "--index", "index.idx")?;
    let query = arg_or_default(args, "--q", "")?;
    if query.trim().is_empty() {
        return Err("--q cannot be empty".to_string());
    }

    let top_k = arg_or_default(args, "--top", "10")?
        .parse::<usize>()
        .map_err(|_| "invalid --top".to_string())?;
    let k1 = arg_or_default(args, "--k1", "1.2")?
        .parse::<f64>()
        .map_err(|_| "invalid --k1".to_string())?;
    let b = arg_or_default(args, "--b", "0.75")?
        .parse::<f64>()
        .map_err(|_| "invalid --b".to_string())?;

    Ok(QueryConfig {
        index_path,
        query,
        top_k,
        k1,
        b,
    })
}

fn parse_profile_config(args: &[String]) -> Result<ProfileConfig, String> {
    let root = arg_or_default(args, "--root", ".")?;
    let workers = arg_or_default(args, "--workers", "0")?
        .parse::<usize>()
        .map_err(|_| "invalid --workers".to_string())?;
    let max_file_size = arg_or_default(args, "--max-file", "1048576")?
        .parse::<u64>()
        .map_err(|_| "invalid --max-file".to_string())?;
    let ext_raw = arg_or_default(args, "--ext", "")?;
    let queries_raw = arg_or_default(args, "--queries", "bm25;index AND query;tokenize OR search")?;
    let top_k = arg_or_default(args, "--top", "10")?
        .parse::<usize>()
        .map_err(|_| "invalid --top".to_string())?;
    let k1 = arg_or_default(args, "--k1", "1.2")?
        .parse::<f64>()
        .map_err(|_| "invalid --k1".to_string())?;
    let b = arg_or_default(args, "--b", "0.75")?
        .parse::<f64>()
        .map_err(|_| "invalid --b".to_string())?;

    let worker_count = if workers == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        workers
    };

    let mut ext_filter = HashSet::new();
    if !ext_raw.trim().is_empty() {
        for ext in ext_raw.split(',') {
            let e = normalize_ext(ext);
            if !e.is_empty() {
                ext_filter.insert(e);
            }
        }
    }

    let queries = queries_raw
        .split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    Ok(ProfileConfig {
        root,
        workers: worker_count.max(1),
        max_file_size,
        ext_filter,
        queries,
        top_k,
        k1,
        b,
    })
}

fn has_flag(args: &[String], key: &str) -> bool {
    args.iter().any(|x| x == key)
}

fn arg_or_default(args: &[String], key: &str, default: &str) -> Result<String, String> {
    let mut i = 0usize;
    while i < args.len() {
        if args[i] == key {
            if i + 1 >= args.len() {
                return Err(format!("missing value for {key}"));
            }
            return Ok(args[i + 1].clone());
        }
        i += 1;
    }
    Ok(default.to_string())
}

fn normalize_ext(ext: &str) -> String {
    let e = ext.trim().to_lowercase();
    if e.is_empty() {
        String::new()
    } else if e.starts_with('.') {
        e
    } else {
        format!(".{e}")
    }
}

fn run_index(cfg: IndexConfig) -> Result<(), String> {
    let root = fs::canonicalize(&cfg.root)
        .map_err(|e| format!("cannot resolve root {}: {e}", cfg.root))?;

    let files = collect_files(&root, &cfg.ext_filter)?;
    let old_index = if cfg.incremental {
        load_index(&cfg.in_index).ok()
    } else {
        None
    };

    let (index, stats) = build_index(
        &root,
        files,
        cfg.workers,
        cfg.max_file_size,
        old_index.as_ref(),
    )?;

    save_index(&cfg.out, &index)?;

    println!(
        "indexed files: {}/{} (reused: {}, skipped: {}), terms: {}, docs: {}, avgdl: {:.2}",
        stats.indexed_files,
        stats.scanned_files,
        stats.reused_files,
        stats.skipped_files,
        index.inverted.len(),
        index.docs.len(),
        index.avg_doc_len
    );
    println!("index written to {}", cfg.out);
    Ok(())
}

fn run_query(cfg: QueryConfig) -> Result<(), String> {
    let index = load_index(&cfg.index_path)?;
    let results = bm25_query(&index, &cfg.query, cfg.top_k, cfg.k1, cfg.b);

    if results.is_empty() {
        println!("no match");
        return Ok(());
    }

    for (i, item) in results.iter().enumerate() {
        let doc = &index.docs[item.doc_id];
        println!("{:>2}. {:.4}  {}", i + 1, item.score, doc.path);
    }

    Ok(())
}

fn run_profile(cfg: ProfileConfig) -> Result<(), String> {
    let root = fs::canonicalize(&cfg.root)
        .map_err(|e| format!("cannot resolve root {}: {e}", cfg.root))?;
    let files = collect_files(&root, &cfg.ext_filter)?;

    let t0 = Instant::now();
    let (index, stats) = build_index(&root, files, cfg.workers, cfg.max_file_size, None)?;
    let index_ms = t0.elapsed().as_millis();

    println!(
        "profile.index files={}/{} skipped={} terms={} docs={} ms={}",
        stats.indexed_files,
        stats.scanned_files,
        stats.skipped_files,
        index.inverted.len(),
        index.docs.len(),
        index_ms
    );

    for q in &cfg.queries {
        let q0 = Instant::now();
        let results = bm25_query(&index, q, cfg.top_k, cfg.k1, cfg.b);
        let q_us = q0.elapsed().as_micros();
        let top = results
            .first()
            .map(|r| format!("{}:{:.4}", index.docs[r.doc_id].path, r.score))
            .unwrap_or_else(|| "none".to_string());
        println!(
            "profile.query q=\"{}\" hits={} us={} top={}",
            q,
            results.len(),
            q_us,
            top
        );
    }

    Ok(())
}

fn collect_files(root: &Path, ext_filter: &HashSet<String>) -> Result<Vec<FileEntry>, String> {
    let ignored_dirs: HashSet<&str> = HashSet::from([
        ".git",
        "node_modules",
        "vendor",
        "target",
        "build",
        "dist",
        ".idea",
        ".vscode",
        ".venv",
        "venv",
    ]);

    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .map_err(|e| format!("failed to read directory {}: {e}", dir.display()))?;

        for entry in entries {
            let entry = match entry {
                Ok(v) => v,
                Err(_) => continue,
            };
            let path = entry.path();
            let ty = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };

            if ty.is_dir() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if ignored_dirs.contains(name.as_ref()) {
                    continue;
                }
                stack.push(path);
                continue;
            }

            if !ty.is_file() {
                continue;
            }

            if !ext_filter.is_empty() {
                let ext = path
                    .extension()
                    .map(|x| format!(".{}", x.to_string_lossy().to_lowercase()))
                    .unwrap_or_default();
                if !ext_filter.contains(&ext) {
                    continue;
                }
            }

            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            let modified_unix = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let rel = relative_or_abs(root, &path);
            files.push(FileEntry {
                abs: path,
                rel,
                size: meta.len(),
                modified_unix,
            });
        }
    }

    Ok(files)
}

fn build_index(
    root: &Path,
    files: Vec<FileEntry>,
    workers: usize,
    max_file_size: u64,
    old_index: Option<&Index>,
) -> Result<(Index, BuildStats), String> {
    let mut stats = BuildStats::default();
    let mut docs = Vec::<Document>::new();
    let mut inverted: HashMap<String, Vec<Posting>> = HashMap::new();
    let mut to_index = Vec::<FileEntry>::new();

    let mut old_doc_map: HashMap<&str, (usize, &Document)> = HashMap::new();
    if let Some(old) = old_index {
        for (i, doc) in old.docs.iter().enumerate() {
            old_doc_map.insert(doc.path.as_str(), (i, doc));
        }
    }

    let mut old_to_new_doc = HashMap::<usize, usize>::new();

    for f in files {
        stats.scanned_files += 1;
        let mut reused = false;
        if let Some((_old_id, old_doc)) = old_doc_map.get(f.rel.as_str()) {
            if old_doc.size == f.size && old_doc.modified_unix == f.modified_unix {
                let new_id = docs.len();
                docs.push((*old_doc).clone());
                old_to_new_doc.insert(*_old_id, new_id);
                stats.reused_files += 1;
                reused = true;
            }
        }
        if !reused {
            to_index.push(f);
        }
    }

    if let Some(old) = old_index {
        for (term, postings) in &old.inverted {
            for p in postings {
                if let Some(&new_doc_id) = old_to_new_doc.get(&p.doc_id) {
                    inverted.entry(term.clone()).or_default().push(Posting {
                        doc_id: new_doc_id,
                        tf: p.tf,
                    });
                }
            }
        }
    }

    let queue = Arc::new(Mutex::new(VecDeque::from(to_index)));
    let (tx, rx) = mpsc::channel::<WorkerMsg>();

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let queue_ref = Arc::clone(&queue);
        let tx_ref = tx.clone();

        let handle = thread::spawn(move || {
            loop {
                let file_opt = {
                    let mut q = queue_ref.lock().expect("queue poisoned");
                    q.pop_front()
                };

                let Some(file) = file_opt else {
                    break;
                };

                match index_one_file(&file.abs, max_file_size) {
                    Ok(Some((token_count, tf_map))) => {
                        let _ = tx_ref.send(WorkerMsg::Indexed {
                            rel_path: file.rel,
                            size: file.size,
                            modified_unix: file.modified_unix,
                            token_count,
                            tf_map,
                        });
                    }
                    _ => {
                        let _ = tx_ref.send(WorkerMsg::Skipped);
                    }
                }
            }
        });

        handles.push(handle);
    }

    drop(tx);

    for msg in rx {
        match msg {
            WorkerMsg::Indexed {
                rel_path,
                size,
                modified_unix,
                token_count,
                tf_map,
            } => {
                let doc_id = docs.len();
                docs.push(Document {
                    path: rel_path,
                    length: token_count,
                    size,
                    modified_unix,
                });

                for (term, tf) in tf_map {
                    inverted
                        .entry(term)
                        .or_default()
                        .push(Posting { doc_id, tf });
                }
                stats.indexed_files += 1;
            }
            WorkerMsg::Skipped => {
                stats.skipped_files += 1;
            }
        }
    }

    for h in handles {
        h.join().map_err(|_| "worker thread panicked".to_string())?;
    }

    let total_len: usize = docs.iter().map(|d| d.length).sum();
    let avg_doc_len = if docs.is_empty() {
        0.0
    } else {
        total_len as f64 / docs.len() as f64
    };

    let created_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let index = Index {
        root: root.to_string_lossy().to_string(),
        created_at_unix,
        docs,
        inverted,
        avg_doc_len,
    };

    Ok((index, stats))
}

fn relative_or_abs(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string_lossy().to_string())
}

fn index_one_file(
    path: &Path,
    max_file_size: u64,
) -> Result<Option<(usize, HashMap<String, usize>)>, String> {
    let metadata =
        fs::metadata(path).map_err(|e| format!("metadata error {}: {e}", path.display()))?;
    if metadata.len() > max_file_size {
        return Ok(None);
    }

    let mut file = File::open(path).map_err(|e| format!("open error {}: {e}", path.display()))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| format!("read error {}: {e}", path.display()))?;

    if is_likely_binary(&data) {
        return Ok(None);
    }

    let content = String::from_utf8_lossy(&data);
    let tokens = tokenize(&content);
    if tokens.is_empty() {
        return Ok(None);
    }

    let mut tf_map: HashMap<String, usize> = HashMap::new();
    for t in &tokens {
        *tf_map.entry(t.clone()).or_insert(0) += 1;
    }

    Ok(Some((tokens.len(), tf_map)))
}

fn is_likely_binary(data: &[u8]) -> bool {
    let sample = if data.len() > 4096 {
        &data[..4096]
    } else {
        data
    };
    sample.contains(&0)
}

fn tokenize(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();

    for ch in s.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            cur.push(ch.to_ascii_lowercase());
        } else if !cur.is_empty() {
            out.push(cur.clone());
            cur.clear();
        }
    }

    if !cur.is_empty() {
        out.push(cur);
    }

    out
}

fn parse_query(query: &str) -> ParsedQuery {
    let raw_parts = query.split_whitespace().collect::<Vec<_>>();
    let has_logic = raw_parts
        .iter()
        .any(|x| x.eq_ignore_ascii_case("AND") || x.eq_ignore_ascii_case("OR"));

    if !has_logic {
        return ParsedQuery {
            mode: QueryMode::OrTerms(tokenize(query)),
        };
    }

    let mut clauses: Vec<Vec<String>> = vec![Vec::new()];
    let mut pending_op = "AND";

    for part in raw_parts {
        if part.eq_ignore_ascii_case("AND") {
            pending_op = "AND";
            continue;
        }
        if part.eq_ignore_ascii_case("OR") {
            pending_op = "OR";
            continue;
        }

        let terms = tokenize(part);
        if terms.is_empty() {
            continue;
        }

        for term in terms {
            if pending_op == "OR" {
                clauses.push(vec![term]);
                pending_op = "AND";
            } else {
                let idx = clauses.len() - 1;
                clauses[idx].push(term);
            }
        }
    }

    clauses.retain(|c| !c.is_empty());
    if clauses.is_empty() {
        ParsedQuery {
            mode: QueryMode::OrTerms(Vec::new()),
        }
    } else {
        ParsedQuery {
            mode: QueryMode::OrOfAndClauses(clauses),
        }
    }
}

fn bm25_query(index: &Index, query: &str, top_k: usize, k1: f64, b: f64) -> Vec<ResultItem> {
    if index.docs.is_empty() || top_k == 0 {
        return Vec::new();
    }

    let parsed = parse_query(query);
    match parsed.mode {
        QueryMode::OrTerms(terms) => bm25_or_terms(index, &terms, top_k, k1, b),
        QueryMode::OrOfAndClauses(clauses) => bm25_or_of_and(index, &clauses, top_k, k1, b),
    }
}

fn bm25_or_terms(
    index: &Index,
    terms: &[String],
    top_k: usize,
    k1: f64,
    b: f64,
) -> Vec<ResultItem> {
    if terms.is_empty() {
        return Vec::new();
    }

    let n = index.docs.len() as f64;
    let avg_dl = if index.avg_doc_len > 0.0 {
        index.avg_doc_len
    } else {
        1.0
    };
    let mut scores: HashMap<usize, f64> = HashMap::new();

    for term in terms {
        let Some(postings) = index.inverted.get(term) else {
            continue;
        };

        let df = postings.len() as f64;
        let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

        for p in postings {
            let dl = index.docs[p.doc_id].length as f64;
            let tf = p.tf as f64;
            let numerator = tf * (k1 + 1.0);
            let denominator = tf + k1 * (1.0 - b + b * dl / avg_dl);
            let score = idf * (numerator / denominator);
            *scores.entry(p.doc_id).or_insert(0.0) += score;
        }
    }

    top_results(index, scores, top_k)
}

fn bm25_or_of_and(
    index: &Index,
    clauses: &[Vec<String>],
    top_k: usize,
    k1: f64,
    b: f64,
) -> Vec<ResultItem> {
    let n = index.docs.len() as f64;
    let avg_dl = if index.avg_doc_len > 0.0 {
        index.avg_doc_len
    } else {
        1.0
    };

    let mut total_scores: HashMap<usize, f64> = HashMap::new();

    for clause in clauses {
        if clause.is_empty() {
            continue;
        }

        let mut candidate: Option<HashSet<usize>> = None;
        let mut term_postings_cache: Vec<(&str, &Vec<Posting>)> = Vec::new();

        for term in clause {
            let Some(postings) = index.inverted.get(term) else {
                candidate = Some(HashSet::new());
                break;
            };
            term_postings_cache.push((term.as_str(), postings));
            let docs_for_term: HashSet<usize> = postings.iter().map(|p| p.doc_id).collect();
            candidate = Some(match candidate {
                Some(curr) => curr.intersection(&docs_for_term).copied().collect(),
                None => docs_for_term,
            });
        }

        let Some(candidates) = candidate else {
            continue;
        };
        if candidates.is_empty() {
            continue;
        }

        for (term, postings) in term_postings_cache {
            let df = postings.len() as f64;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

            for p in postings {
                if !candidates.contains(&p.doc_id) {
                    continue;
                }
                let dl = index.docs[p.doc_id].length as f64;
                let tf = p.tf as f64;
                let numerator = tf * (k1 + 1.0);
                let denominator = tf + k1 * (1.0 - b + b * dl / avg_dl);
                let score = idf * (numerator / denominator);
                *total_scores.entry(p.doc_id).or_insert(0.0) += score;
            }

            let _ = term;
        }
    }

    top_results(index, total_scores, top_k)
}

fn top_results(index: &Index, scores: HashMap<usize, f64>, top_k: usize) -> Vec<ResultItem> {
    let mut results: Vec<ResultItem> = scores
        .into_iter()
        .map(|(doc_id, score)| ResultItem { doc_id, score })
        .collect();

    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| index.docs[a.doc_id].path.cmp(&index.docs[b.doc_id].path))
    });

    if results.len() > top_k {
        results.truncate(top_k);
    }
    results
}

fn save_index(path: &str, index: &Index) -> Result<(), String> {
    let mut f = File::create(path).map_err(|e| format!("create index file failed: {e}"))?;

    writeln!(f, "CODESEARCH_INDEX_V2").map_err(|e| e.to_string())?;
    writeln!(f, "ROOT\t{}", escape_field(&index.root)).map_err(|e| e.to_string())?;
    writeln!(f, "CREATED_AT_UNIX\t{}", index.created_at_unix).map_err(|e| e.to_string())?;
    writeln!(f, "DOCS\t{}", index.docs.len()).map_err(|e| e.to_string())?;

    for doc in &index.docs {
        writeln!(
            f,
            "{}\t{}\t{}\t{}",
            doc.length,
            doc.size,
            doc.modified_unix,
            escape_field(&doc.path)
        )
        .map_err(|e| e.to_string())?;
    }

    writeln!(f, "TERMS\t{}", index.inverted.len()).map_err(|e| e.to_string())?;
    let mut terms: Vec<&String> = index.inverted.keys().collect();
    terms.sort();

    for term in terms {
        let postings = index.inverted.get(term).expect("term exists");
        write!(f, "{}\t", escape_field(term)).map_err(|e| e.to_string())?;
        for (i, p) in postings.iter().enumerate() {
            if i > 0 {
                write!(f, ",").map_err(|e| e.to_string())?;
            }
            write!(f, "{}:{}", p.doc_id, p.tf).map_err(|e| e.to_string())?;
        }
        writeln!(f).map_err(|e| e.to_string())?;
    }

    Ok(())
}

fn load_index(path: &str) -> Result<Index, String> {
    let file = File::open(path).map_err(|e| format!("open index file failed: {e}"))?;
    let mut reader = BufReader::new(file);

    let mut line = String::new();
    reader.read_line(&mut line).map_err(|e| e.to_string())?;
    let header = line.trim_end();
    if header != "CODESEARCH_INDEX_V2" && header != "CODESEARCH_INDEX_V1" {
        return Err("invalid index format header".to_string());
    }
    let v1 = header == "CODESEARCH_INDEX_V1";

    line.clear();
    reader.read_line(&mut line).map_err(|e| e.to_string())?;
    let root = parse_kv_line(&line, "ROOT")?;

    line.clear();
    reader.read_line(&mut line).map_err(|e| e.to_string())?;
    let created_str = parse_kv_line(&line, "CREATED_AT_UNIX")?;
    let created_at_unix = created_str
        .parse::<u64>()
        .map_err(|_| "invalid CREATED_AT_UNIX".to_string())?;

    line.clear();
    reader.read_line(&mut line).map_err(|e| e.to_string())?;
    let docs_count = parse_kv_line(&line, "DOCS")?
        .parse::<usize>()
        .map_err(|_| "invalid DOCS count".to_string())?;

    let mut docs = Vec::with_capacity(docs_count);
    for _ in 0..docs_count {
        line.clear();
        reader.read_line(&mut line).map_err(|e| e.to_string())?;
        if v1 {
            let parts: Vec<&str> = line.trim_end().splitn(2, '\t').collect();
            if parts.len() != 2 {
                return Err("invalid v1 doc line".to_string());
            }
            let length = parts[0]
                .parse::<usize>()
                .map_err(|_| "invalid doc length".to_string())?;
            let path = unescape_field(parts[1]);
            docs.push(Document {
                path,
                length,
                size: 0,
                modified_unix: 0,
            });
        } else {
            let parts: Vec<&str> = line.trim_end().splitn(4, '\t').collect();
            if parts.len() != 4 {
                return Err("invalid v2 doc line".to_string());
            }
            let length = parts[0]
                .parse::<usize>()
                .map_err(|_| "invalid doc length".to_string())?;
            let size = parts[1]
                .parse::<u64>()
                .map_err(|_| "invalid doc size".to_string())?;
            let modified_unix = parts[2]
                .parse::<u64>()
                .map_err(|_| "invalid modified_unix".to_string())?;
            let path = unescape_field(parts[3]);
            docs.push(Document {
                path,
                length,
                size,
                modified_unix,
            });
        }
    }

    line.clear();
    reader.read_line(&mut line).map_err(|e| e.to_string())?;
    let terms_count = parse_kv_line(&line, "TERMS")?
        .parse::<usize>()
        .map_err(|_| "invalid TERMS count".to_string())?;

    let mut inverted = HashMap::with_capacity(terms_count);

    for _ in 0..terms_count {
        line.clear();
        if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            return Err("unexpected EOF while reading terms".to_string());
        }
        let parts: Vec<&str> = line.trim_end().splitn(2, '\t').collect();
        if parts.len() != 2 {
            return Err("invalid term line".to_string());
        }

        let term = unescape_field(parts[0]);
        let mut postings = Vec::new();
        if !parts[1].is_empty() {
            for item in parts[1].split(',') {
                let kv: Vec<&str> = item.splitn(2, ':').collect();
                if kv.len() != 2 {
                    continue;
                }
                let doc_id = match kv[0].parse::<usize>() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let tf = match kv[1].parse::<usize>() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if doc_id < docs.len() {
                    postings.push(Posting { doc_id, tf });
                }
            }
        }
        inverted.insert(term, postings);
    }

    let total_len: usize = docs.iter().map(|d| d.length).sum();
    let avg_doc_len = if docs.is_empty() {
        0.0
    } else {
        total_len as f64 / docs.len() as f64
    };

    Ok(Index {
        root,
        created_at_unix,
        docs,
        inverted,
        avg_doc_len,
    })
}

fn parse_kv_line(line: &str, key: &str) -> Result<String, String> {
    let s = line.trim_end();
    let parts: Vec<&str> = s.splitn(2, '\t').collect();
    if parts.len() != 2 || parts[0] != key {
        return Err(format!("invalid line for key {key}"));
    }
    Ok(unescape_field(parts[1]))
}

fn escape_field(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

fn unescape_field(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}
