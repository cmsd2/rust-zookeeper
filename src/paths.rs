pub fn make_path(parent: &str, child: &str) -> String {
    let mut result = parent.to_owned();
    if parent.len() == 0 || parent.chars().last().unwrap() != '/' {
        result.push('/');
    }
    result.push_str(child);
    result
}

/// splits a path into directory and name components
pub fn split_path<'a>(path: &'a str) -> (&'a str, &'a str) {
    let slash_pos = path.rfind('/');

    match slash_pos {
        Some(pos) => {
            let name_pos = pos + 1;
            (&path[..pos], &path[name_pos..])
        }
        None => {
            ("", "")
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    
    #[test]
    pub fn test_make_path() {
        assert_eq!(make_path("", ""), "/");
        assert_eq!(make_path("/", ""), "/");
        assert_eq!(make_path("/foo", "bar"), "/foo/bar");
        assert_eq!(make_path("/foo/", "bar"), "/foo/bar");
    }

    #[test]
    pub fn test_split_path() {
        assert_eq!(split_path("/"), ("", ""));
        assert_eq!(split_path("/foo"), ("", "foo"));
        assert_eq!(split_path("/foo/bar"), ("/foo", "bar"));
    }
}
