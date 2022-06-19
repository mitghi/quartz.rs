const MONTHS: [&'static str; 13] = [
    "0", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];

const DAYS: [&'static str; 8] = ["0", "SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

const DAYS_IN_MONTH: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

const SPECIAL_CRON_TAGS: &[(&'static str, &'static str)] = &[
    ("@daily", "0 0 0 * * *"),
    ("@hourly", "0 0 * * * *"),
    ("@monthly", "0 0 0 1 * *"),
    ("@weekly", "0 0 0 * * 1"),
    ("@yearly", "0 0 0 1 1 *"),
];

const E_PARSE_CRON_RANGE_ERROR: &'static str = "Invalid range";

#[derive(Debug, Clone)]
pub struct TokenError(String);

#[derive(Debug, Clone)]
pub struct ParseError(String);

#[derive(Debug)]
struct CronField(Vec<i64>);

enum FieldIndex {
    SECOND,
    MINUTE,
    HOUR,
    DAYOFWEEK,
    MONTH,
    DAYOFMONTH,
    YEAR,
}

impl CronField {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn incr(&mut self, v: i64) {
        self.0 = self.0.iter().map(|x| x + v).collect();
    }
}

fn get_special_cron_tag(key: &str) -> Option<&'static str> {
    let result = SPECIAL_CRON_TAGS
        .binary_search_by(|(k, _)| k.cmp(&key))
        .map(|v| SPECIAL_CRON_TAGS[v].1);

    return match result {
        Ok(value) => Some(value),
        Err(_) => None,
    };
}

fn parse_list_field<'a>(field: &'a str, translate: &'a [&'a str]) -> Result<CronField, ParseError> {
    let items: Vec<&str> = field.split(",").collect();
    let mut converted_items: Vec<i64> = items
        .iter()
        .by_ref()
        .map_while(|x| x.parse::<i64>().ok())
        .collect();

    if converted_items.len() != items.len() {
        let mut indexes: Vec<i64> = Vec::new();
        for item in items.iter() {
            let u_item = item.to_uppercase();
            if let Some(index) = translate.iter().position(|&p| *p == *&u_item) {
                indexes.push(index as i64);
            } else {
                return Err(ParseError("Unable to match indexes".to_string()));
            }
        }
        converted_items = indexes;
    }

    converted_items.sort();
    Ok(CronField(converted_items))
}

fn find_index_in(term: &str, bin: &[&str]) -> i64 {
    let u_term = term.to_uppercase();
    for (i, item) in bin.iter().enumerate() {
        if item.to_uppercase() == u_term {
            return i as i64;
        }
    }
    return -1;
}

fn parse_step_field(
    field: &str,
    min: i64,
    max: i64,
    translate: &[&str],
) -> Result<CronField, ParseError> {
    let mut buffer: Vec<i64>;
    let mut items: Vec<&str> = field.split("/").collect();
    if items.len() != 2 {
        return Err(ParseError("Invalid step range".to_string()));
    }
    let v: String = min.to_string();
    if items[0] == "*" {
        items[0] = v.as_str();
    };

    let from: i64 = {
        let _value: Result<i64, _> = items[0].parse();
        if _value.is_ok() {
            _value.ok().unwrap()
        } else {
            find_index_in(&items[0], &translate)
        }
    };

    let step: i64 = {
        let _value: Result<i64, _> = items[1].parse();
        if _value.is_ok() {
            _value.ok().unwrap()
        } else {
            return Err(ParseError("Invalid step in range".to_string()));
        }
    };

    if !in_range(from, min, max) || max < from {
        return Err(ParseError("Invalid range".to_string()));
    };

    buffer = Vec::new();
    for i in (from..=max).step_by(step as usize) {
        buffer.push(i);
    }

    Ok(CronField(buffer))
}

fn parse_range_field(
    field: &str,
    min: i64,
    max: i64,
    translate: &[&str],
) -> Result<CronField, ParseError> {
    let items: Vec<&str> = field.split("-").collect();
    if items.len() != 2 {
        return Err(ParseError("Invalid range".to_string()));
    }

    let from: i64 = {
        let _value: Result<i64, _> = items[0].parse();
        if _value.is_ok() {
            _value.ok().unwrap()
        } else {
            find_index_in(&items[0], &translate)
        }
    };

    let to: i64 = {
        let _value: Result<i64, _> = items[1].parse();
        if _value.is_ok() {
            _value.ok().unwrap()
        } else {
            find_index_in(&items[1], &translate)
        }
    };

    if !in_range(from, min, max) || !in_range(to, min, max) || to < from {
        return Err(ParseError("Invalid min/max range".to_string()));
    }

    Ok(CronField((from..=to).collect()))
}

fn validate_cron_expr<'a>(expression: &'a str) -> Result<Vec<&'a str>, TokenError> {
    let mut tokens: Vec<&str>;
    let mut tlen: usize;

    if let Some(result) = get_special_cron_tag(&expression) {
        tokens = result.split(" ").collect();
    } else {
        tokens = expression.split(" ").collect();
    }

    match tokens.len() {
        usize::MIN..=5 | 8..=usize::MAX => {
            return Err(TokenError("Invalid length".to_string()));
        }
        6 => {
            tokens.push("*");
        }
        _ => {}
    };

    if (tokens[3] != "?" && tokens[3] != "*") && (tokens[5] != "?" && tokens[5] != "*") {
        return Err(TokenError("Day field is set twice".to_string()));
    }

    if tokens[6] != "*" {
        return Err(TokenError(
            "Year field is not supported, use asterisk".to_string(),
        ));
    }

    Ok(tokens)
}

#[inline(always)]
fn in_range(value: i64, min: i64, max: i64) -> bool {
    if value >= min && value <= max {
        return true;
    }
    return false;
}

fn build_cron_field(tokens: Vec<&str>) -> Result<Vec<CronField>, ParseError> {
    let mut fields: Vec<CronField> = Vec::with_capacity(7);

    match parse_field(&tokens[0], 0, 59, None) {
        Ok(result) => {
            fields.push(result);
        }
        Err(err) => {
            return Err(err);
        }
    };

    match parse_field(&tokens[1], 0, 59, None) {
        Ok(result) => {
            fields.push(result);
        }
        Err(err) => {
            return Err(err);
        }
    };

    match parse_field(&tokens[2], 0, 23, None) {
        Ok(result) => {
            fields.push(result);
        }
        Err(err) => {
            return Err(err);
        }
    };

    match parse_field(&tokens[3], 1, 31, None) {
        Ok(result) => {
            fields.push(result);
        }
        Err(err) => {
            return Err(err);
        }
    };

    match parse_field(&tokens[4], 1, 12, Some(&MONTHS.as_slice())) {
        Ok(result) => {
            fields.push(result);
        }
        Err(err) => {
            return Err(err);
        }
    };

    match parse_field(&tokens[5], 1, 7, Some(&DAYS.as_slice())) {
        Ok(result) => {
            fields.push(result);
            fields[5].incr(-1);
        }
        Err(err) => {
            return Err(err);
        }
    };

    match parse_field(&tokens[6], 1970, 1970 * 2, None) {
        Ok(result) => {
            fields.push(result);
        }
        Err(err) => {
            return Err(err);
        }
    };

    Ok(fields)
}

fn parse_field(
    field: &str,
    min: i64,
    max: i64,
    translate: Option<&[&str]>,
) -> Result<CronField, ParseError> {
    let mut dict: &[&str] = if let Some(translate) = translate {
        translate
    } else {
        &[]
    };

    match field {
        "*" | "?" => {
            return Ok(CronField(vec![]));
        }
        _ => {}
    };

    {
        let result: Result<i64, _> = field.parse();
        match result {
            Ok(value) => {
                if in_range(*&value, min, max) {
                    return Ok(CronField(vec![value]));
                } else {
                    return Err(ParseError("Range Error".to_string()));
                }
            }
            _ => {}
        };
    }

    if field.contains(",") {
        return parse_list_field(&field, &dict);
    }

    if field.contains("-") {
        return parse_range_field(&field, min, max, &dict);
    }

    if field.contains("/") {
        return parse_step_field(&field, min, max, &dict);
    }

    if dict.len() > 0 {
        let index = find_index_in(&field, &dict);
        if index >= 0 {
            if in_range(*&index, min, max) {
                return Ok(CronField(vec![index]));
            }
            return Err(ParseError(
                "Cron literal min/max validation error".to_string(),
            ));
        }
    }

    Err(ParseError("Cron parse error".to_string()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn cron_test_get_special_cron_tag() {
        assert_eq!(get_special_cron_tag("@daily").unwrap(), "0 0 0 * * *");
        assert_eq!(get_special_cron_tag("@yearly").unwrap(), "0 0 0 1 1 *");
        assert_eq!(get_special_cron_tag("@monthly").unwrap(), "0 0 0 1 * *");
        assert_eq!(get_special_cron_tag("@weekly").unwrap(), "0 0 0 * * 1");
        assert_eq!(get_special_cron_tag("@hourly").unwrap(), "0 0 * * * *");
        assert_eq!(get_special_cron_tag("non existent").is_some(), false);
    }

    #[test]
    fn cron_test_parse_list_field() {
        let input: &str = "4,6";
        let output = parse_list_field(&input, &DAYS);

        assert_eq!(output.is_ok(), true);

        println!("{:?}", output.ok().unwrap());
    }

    #[test]
    fn cron_test_parse_range_field() {
        let input: &str = "2-8";
        let output = parse_range_field(&input, 1, 60, &DAYS);
        println!("{:?}", output.ok().unwrap());
    }

    #[test]
    fn cron_test_build_cron_field() {
        let input: &str = "20 40 16 2/4 4 *";
        let tokens = validate_cron_expr(&input);

        println!("{:?}", &tokens);

        let output = build_cron_field(tokens.unwrap());

        println!("{:?}", output);
    }
}
