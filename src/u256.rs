use pgrx::prelude::*;

#[pg_schema]
#[allow(non_snake_case)]
mod U256 {
    use alloy::core::hex;
    use alloy::core::primitives::U256;

    use pgrx::prelude::*;

    #[pg_extern(name = "parse", immutable, parallel_safe)]
    fn parse(string: &str) -> pgrx::AnyNumeric {
        pgrx::AnyNumeric::try_from(
            U256::from_be_slice(&hex::decode(string).expect("to be big endian hex"))
                .to_string()
                .as_str(),
        )
        .expect("Failed to convert U256 to AnyNumeric")
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::datum::DatumWithOid;
    use pgrx::prelude::*;

    use anyhow::Result;
    use std::str::FromStr;

    #[pg_test]
    fn u256_parse() -> Result<()> {
        let data = "0000000000000000000000000000000000000000000024be4b7d04405304e8de";

        let parsed = Spi::get_one_with_args::<pgrx::AnyNumeric>(
            "SELECT U256.parse($1);",
            &vec![DatumWithOid::from(data)],
        )?;

        assert_eq!(
            parsed,
            Some(pgrx::AnyNumeric::from_str("173515514265911293176030")?)
        );
        Ok(())
    }
}
