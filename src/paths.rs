use regex::Regex;

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

            let dir = if pos == 0 {
                "/"
            } else {
                &path[..pos]
            };

            let name = &path[name_pos..];

            (dir, name)
        }
        None => {
            ("", "")
        }
    }
}

/// splits a name of the form _c_321431-1212-1212-3324932-name00001
/// into the 2-tuple (uuid, name),
pub fn split_protected_name<'a>(name: &'a str) -> Option<(&'a str, &'a str)> {
    let re = Regex::new(r"^_c_([:xdigit:]{8}-[:xdigit:]{4}-[:xdigit:]{4}-[:xdigit:]{4}-[:xdigit:]{12})-(.*)$").unwrap();
    let maybe_captures = re.captures(name);
    maybe_captures.and_then(|captures| {
        if captures.at(1).is_some() && captures.at(2).is_some() {
            Some((captures.at(1).unwrap(), captures.at(2).unwrap()))
        } else {
            None
        }
    })
}

/// tries to match the given protected name against the uuid and name
/// returns the suffix after the name if it matches
/// returns none otherwise
/// for a name e.g. "lock" and uuid e.g. "32143121-1212-1212-1212-332493212345"
/// and matching protected name "_c_32143121-1212-1212-1212-332493212345-lock000001"
/// the suffix is 000001
pub fn split_protected_name_suffix<'a>(name: &str, protected_name: &'a str) -> Option<(&'a str, &'a str, &'a str)> {
    let name_len = name.len();
        
    split_protected_name(protected_name).map(|parts| {
        let (uuid, name_and_suffix) = parts;
                                             
        if name_and_suffix.starts_with(name) {
            (uuid, &name_and_suffix[0..name_len], &name_and_suffix[name_len..])
        } else {
            (uuid, name_and_suffix, "")
        }
    })
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
        assert_eq!(split_path("/"), ("/", ""));
        assert_eq!(split_path("/foo"), ("/", "foo"));
        assert_eq!(split_path("/foo/bar"), ("/foo", "bar"));
    }

    #[test]
    pub fn test_split_protected_name() {
        assert_eq!(split_protected_name("foo"), None);
        assert_eq!(split_protected_name("_c_12f4a678-1a34-1b34-1c34-1a3f56789012-name001"), Some(("12f4a678-1a34-1b34-1c34-1a3f56789012", "name001")));
        assert_eq!(split_protected_name("_c_12345678-1234-1234-1234-123456789012-name"), Some(("12345678-1234-1234-1234-123456789012", "name")));
    }

    #[test]
    pub fn test_split_protected_name_suffix() {
        assert_eq!(split_protected_name_suffix("lock", "blah"), None);
        assert_eq!(split_protected_name_suffix("lock", "_c_uuid-lock"), None);
        assert_eq!(split_protected_name_suffix("name", "_c_12345678-1234-1234-1234-123456789012-name"), Some(("12345678-1234-1234-1234-123456789012", "name", "")));
        assert_eq!(split_protected_name_suffix("name", "_c_12345678-1234-1234-1234-123456789012-name00001"), Some(("12345678-1234-1234-1234-123456789012", "name", "00001")));
    }
}
