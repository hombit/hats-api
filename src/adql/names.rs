//! Writing a name the way ADQL requires it to be written.
//!
//! A column out of somebody's parquet file may be called anything; ADQL's grammar admits a
//! letter followed by letters, digits and underscores. A name outside that has to be
//! written as a delimited identifier, and TAP §4.3 says the name a service *publishes*
//! carries the quotes — so a client that copies it out of `TAP_SCHEMA` gets something it
//! can put in a `SELECT`.
//!
//! `_healpix_29` is the case every HATS catalog has, its leading underscore being what
//! ADQL's grammar does not admit.
//!
//! **The reserved words are deliberately not part of this.** ADQL inherits SQL92's list,
//! which holds `DEC` — and `SELECT ra, dec` is what every astronomy query ever written
//! says, every reference service publishes `dec` bare, and `taplint` does not flag it.
//! Delimiting on that list would produce published names nobody else produces, which is
//! the interoperability harm the rule exists to avoid rather than a stricter reading of
//! it. What is applied is the shape of the name and nothing else.

/// Whether a name can be written in ADQL without quotes.
pub fn is_plain(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && characters.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The name as a query has to write it: bare where ADQL admits it, delimited otherwise.
///
/// This is what a service *publishes*, and TAP §4.3 asks the published name to be the
/// string recommended for querying — so the quotes are part of it where they are needed.
/// They are not added where they are not: a delimited identifier matches case-sensitively,
/// and a browser showing every column in quotes is one nobody reads.
///
/// **A dotted path is judged part by part.** The dot is structure rather than part of a
/// name, so `"lightcurve.mag"` would be one delimited identifier naming no field.
pub fn as_written(name: &str) -> String {
    name.split('.')
        .map(|part| match is_plain(part) {
            true => part.to_owned(),
            // A quote inside a delimited identifier is written twice, which is SQL's own
            // escape and the only one there is.
            false => format!("\"{}\"", part.replace('"', "\"\"")),
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case every HATS catalog has, and the ones a catalog's own columns hit.
    #[test]
    fn a_name_adql_cannot_write_bare_carries_its_quotes() {
        assert_eq!(as_written("ra"), "ra");
        assert_eq!(as_written("phot_g_mean_mag"), "phot_g_mean_mag");
        assert_eq!(as_written("Gmag"), "Gmag");
        // A leading underscore, which ADQL's grammar does not admit.
        assert_eq!(as_written("_healpix_29"), "\"_healpix_29\"");
        // A reserved word is left bare: `dec` is one, and delimiting it would publish a
        // name no other service publishes for the column every query reads.
        assert_eq!(as_written("dec"), "dec");
        assert_eq!(as_written("size"), "size");
        // A digit cannot lead one either.
        assert_eq!(as_written("2mass_id"), "\"2mass_id\"");
        assert_eq!(as_written("mag-err"), "\"mag-err\"");
        // A quote of its own is doubled, which is SQL's only escape.
        assert_eq!(as_written("od\"d"), "\"od\"\"d\"");
    }

    /// The dot is structure. Quoting the whole path would name no field at all.
    #[test]
    fn a_dotted_path_is_quoted_part_by_part() {
        assert_eq!(as_written("lightcurve.mag"), "lightcurve.mag");
        assert_eq!(as_written("_lc.mag"), "\"_lc\".mag");
        assert_eq!(as_written("lc.2mass"), "lc.\"2mass\"");
    }
}
