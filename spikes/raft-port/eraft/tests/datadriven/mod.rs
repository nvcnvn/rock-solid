// Minimal port of github.com/cockroachdb/datadriven v1.0.2 (line_parser.go,
// test_data_reader.go, datadriven.go rewrite mode). Apache License 2.0.
//
// Only rewrite mode is implemented: each directive's actual output replaces
// the expected output, and the caller compares the rewritten file with the
// original byte for byte.

#![allow(
    dead_code,
    reason = "shared test module; not every test binary uses every item"
)]

/// One `key`, `key=val` or `key=(a, b)` argument.
#[derive(Clone, Debug, Default)]
pub struct CmdArg {
    pub key: String,
    pub vals: Vec<String>,
}

/// One directive.
#[derive(Clone, Debug, Default)]
pub struct TestData {
    pub pos: String,
    pub cmd: String,
    pub cmd_args: Vec<CmdArg>,
    pub input: String,
    pub expected: String,
}

impl TestData {
    /// Returns the values of argument `key`.
    pub fn arg(&self, key: &str) -> Option<&CmdArg> {
        self.cmd_args.iter().find(|a| a.key == key)
    }
}

/// Parses a directive line (`ParseLine`).
pub fn parse_line(line: &str) -> Result<(String, Vec<CmdArg>), String> {
    let orig = line.trim();
    let mut line = orig;
    if line.is_empty() {
        return Ok((String::new(), Vec::new()));
    }
    let err = |line: &str| {
        format!(
            "cannot parse directive at column {}: {}",
            orig.len() - line.len() + 1,
            orig
        )
    };
    fn until<'a>(line: &mut &'a str, chars: &[char]) -> &'a str {
        let idx = line.find(chars).unwrap_or(line.len());
        let (res, rest) = line.split_at(idx);
        *line = rest;
        res
    }
    let cmd = until(&mut line, &[' ']).to_string();
    if cmd.is_empty() {
        return Err(err(line));
    }
    line = line.trim();
    let mut args = Vec::new();
    while !line.is_empty() {
        let mut arg = CmdArg {
            key: until(&mut line, &[' ', '=']).to_string(),
            vals: Vec::new(),
        };
        if arg.key.is_empty() {
            return Err(err(line));
        }
        if let Some(rest) = line.strip_prefix('=') {
            line = rest;
            if line.is_empty() || line.starts_with(' ') {
                arg.vals = vec![String::new()];
            } else if !line.starts_with('(') {
                arg.vals = vec![until(&mut line, &[' ']).to_string()];
            } else {
                let b = line.as_bytes();
                let mut pos = 1;
                let mut nest = 1;
                let mut last = pos;
                while nest > 0 {
                    if pos == b.len() {
                        return Err(err(line));
                    }
                    let c = b[pos];
                    pos += 1;
                    match c {
                        b',' if nest == 1 => {
                            arg.vals.push(line[last..pos - 1].to_string());
                            while pos < b.len() && b[pos] == b' ' {
                                pos += 1;
                            }
                            last = pos;
                        }
                        b'(' => nest += 1,
                        b')' => nest -= 1,
                        _ => {}
                    }
                }
                arg.vals.push(line[last..pos - 1].to_string());
                line = line[pos..].trim();
            }
        }
        args.push(arg);
        line = line.trim();
    }
    Ok((cmd, args))
}

fn has_blank_line(s: &str) -> bool {
    s.split_inclusive('\n')
        .any(|l| l.ends_with('\n') && l[..l.len() - 1].chars().all(|c| c == ' ' || c == '\t'))
}

/// Runs every directive in `src` through `f` and returns the rewritten file.
pub fn rewrite(name: &str, src: &str, mut f: impl FnMut(&TestData) -> String) -> String {
    let lines: Vec<&str> = src.lines().collect();
    let mut i = 0;
    let mut out = String::new();
    let emit = |out: &mut String, s: &str| {
        out.push_str(s);
        out.push('\n');
    };
    while i < lines.len() {
        let mut line = lines[i].to_string();
        i += 1;
        emit(&mut out, &line);
        let pos = format!("{name}:{i}");
        let trimmed = line.trim().to_string();
        if trimmed.starts_with('#') {
            continue;
        }
        line = trimmed;
        while line.ends_with('\\') && i < lines.len() {
            let next = lines[i];
            i += 1;
            emit(&mut out, next);
            line = format!("{} {}", line.trim_end_matches('\\'), next.trim());
        }
        let (cmd, cmd_args) = parse_line(&line).unwrap_or_else(|e| panic!("{pos}: {e}"));
        if cmd.is_empty() {
            continue;
        }
        let mut input = String::new();
        let mut separator = false;
        while i < lines.len() {
            let l = lines[i];
            i += 1;
            if l == "----" {
                separator = true;
                break;
            }
            emit(&mut out, l);
            input.push_str(l);
            input.push('\n');
        }
        let mut expected = String::new();
        if separator {
            let mut allow_blank = false;
            let mut first = None;
            if i < lines.len() {
                first = Some(lines[i]);
                i += 1;
                if lines[i - 1] == "----" {
                    allow_blank = true;
                }
            }
            if allow_blank {
                while i < lines.len() {
                    let l = lines[i];
                    i += 1;
                    if l == "----" && i < lines.len() {
                        let l2 = lines[i];
                        i += 1;
                        if l2 == "----" {
                            if i < lines.len() {
                                i += 1;
                            }
                            break;
                        }
                        expected.push_str(l);
                        expected.push('\n');
                        expected.push_str(l2);
                        expected.push('\n');
                        continue;
                    }
                    expected.push_str(l);
                    expected.push('\n');
                }
            } else if let Some(mut l) = first {
                loop {
                    if l.trim().is_empty() {
                        break;
                    }
                    expected.push_str(l);
                    expected.push('\n');
                    if i >= lines.len() {
                        break;
                    }
                    l = lines[i];
                    i += 1;
                }
            }
        }
        let td = TestData {
            pos,
            cmd,
            cmd_args,
            input: input.trim().to_string(),
            expected,
        };
        let mut actual = f(&td);
        if !actual.is_empty() && !actual.ends_with('\n') {
            actual.push('\n');
        }
        emit(&mut out, "----");
        if has_blank_line(&actual) {
            emit(&mut out, "----");
            out.push_str(&actual);
            emit(&mut out, "----");
            emit(&mut out, "----");
            emit(&mut out, "");
        } else {
            emit(&mut out, &actual);
        }
    }
    if out.ends_with("\n\n") && out.len() > 2 {
        out.pop();
    }
    out
}
