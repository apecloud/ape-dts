use std::collections::HashMap;
use std::str::FromStr;

use dt_common::meta::col_value::ColValue;
use dt_common::meta::row_data::RowData;
use dt_common::meta::row_type::RowType;
use mlua::{IntoLua, Lua};

type PreservedColValues = HashMap<String, ColValue>;

pub struct LuaProcessor {
    pub lua_code: String,
}

impl LuaProcessor {
    pub fn process(&self, data: Vec<RowData>) -> anyhow::Result<Vec<RowData>> {
        let mut new_data = Vec::new();
        let lua = Lua::new();

        for row_data in data {
            // to lua
            let (lua_before, blob_before) =
                self.col_values_to_lua_table(row_data.before.clone(), &lua)?;
            let (lua_after, blob_after) =
                self.col_values_to_lua_table(row_data.after.clone(), &lua)?;

            lua.globals().set("before", lua_before)?;
            lua.globals().set("after", lua_after)?;
            lua.globals().set("schema", row_data.schema)?;
            lua.globals().set("tb", row_data.tb)?;
            lua.globals()
                .set("row_type", row_data.row_type.to_string())?;

            // execute lua
            lua.load(&self.lua_code).exec()?;

            // row filtered
            let row_type: String = lua.globals().get("row_type")?;
            if row_type.is_empty() {
                continue;
            }

            // from lua
            let lua_before: mlua::Table = lua.globals().get("before")?;
            let lua_after: mlua::Table = lua.globals().get("after")?;
            let before = self.lua_table_to_col_values(lua_before, blob_before)?;
            let after = self.lua_table_to_col_values(lua_after, blob_after)?;

            let schema = lua.globals().get("schema")?;
            let tb = lua.globals().get("tb")?;
            let row_type = RowType::from_str(&row_type)?;
            let new_row_data = RowData::new(schema, tb, row_type, before, after);
            new_data.push(new_row_data);
        }

        Ok(new_data)
    }

    fn col_values_to_lua_table<'lua>(
        &'lua self,
        col_values: Option<HashMap<String, ColValue>>,
        lua: &'lua mlua::Lua,
    ) -> anyhow::Result<(mlua::Table<'lua>, PreservedColValues)> {
        let lua_table = lua.create_table()?;
        let mut preserved_col_values = HashMap::new();

        if let Some(map) = col_values {
            for (key, col_value) in map {
                let (lua_value, preserved_col_value) =
                    self.encode_col_value_for_lua(col_value, lua)?;
                if let Some(col_value) = preserved_col_value {
                    preserved_col_values.insert(key.clone(), col_value);
                }
                lua_table.set(key, lua_value)?;
            }
        }

        Ok((lua_table, preserved_col_values))
    }

    fn lua_table_to_col_values(
        &self,
        lua_table: mlua::Table,
        preserved_col_values: PreservedColValues,
    ) -> anyhow::Result<Option<HashMap<String, ColValue>>> {
        if lua_table.is_empty() {
            return Ok(None);
        }

        let mut map = HashMap::new();
        for pair in lua_table.pairs() {
            let pair = pair?;
            let lua_value: mlua::Value = pair.1;
            let col_value = self.lua_value_to_col_value(lua_value)?;
            map.insert(pair.0, col_value);
        }

        self.restore_preserved_col_values(&mut map, preserved_col_values);

        Ok(Some(map))
    }

    fn restore_preserved_col_values(
        &self,
        map: &mut HashMap<String, ColValue>,
        preserved_col_values: PreservedColValues,
    ) {
        for (col, preserved_col_value) in preserved_col_values {
            // If a column was removed in lua, it should not exist in the table anymore.
            // If it still exists as NULL/None, lua did not change it, so restore the original value.
            if matches!(map.get(&col), Some(ColValue::None)) {
                map.insert(col, preserved_col_value);
            }
        }
    }

    fn encode_col_value_for_lua<'lua>(
        &'lua self,
        col_value: ColValue,
        lua: &'lua mlua::Lua,
    ) -> anyhow::Result<(mlua::Value<'lua>, Option<ColValue>)> {
        match col_value {
            // Blob columns are intentionally read-only in Lua. Preserve the original value and
            // expose NULL so a no-op script keeps the source bytes untouched.
            ColValue::Blob(v) => Ok((mlua::Value::NULL, Some(ColValue::Blob(v)))),

            // MySQL CDC text columns may arrive as RawString. If bytes are valid UTF-8, expose
            // them as normal Lua strings. Otherwise preserve the original bytes and expose NULL.
            ColValue::RawString(v) => match ColValue::RawString(v.clone()).to_utf8_string() {
                Some(s) => Ok((mlua::Value::String(lua.create_string(&s)?), None)),
                None => Ok((mlua::Value::NULL, Some(ColValue::RawString(v)))),
            },

            _ => Ok((self.col_value_to_lua_value(col_value, lua)?, None)),
        }
    }

    fn col_value_to_lua_value<'lua>(
        &'lua self,
        col_value: ColValue,
        lua: &'lua mlua::Lua,
    ) -> anyhow::Result<mlua::Value<'lua>> {
        let lua_value = match col_value {
            ColValue::Bool(v) => mlua::Value::Boolean(v),
            ColValue::Tiny(v) => mlua::Value::Integer(v as i64),
            ColValue::UnsignedTiny(v) => mlua::Value::Integer(v as i64),
            ColValue::Short(v) => mlua::Value::Integer(v as i64),
            ColValue::UnsignedShort(v) => mlua::Value::Integer(v as i64),
            ColValue::Long(v) => mlua::Value::Integer(v as i64),
            ColValue::UnsignedLong(v) => mlua::Value::Integer(v as i64),
            ColValue::LongLong(v) => mlua::Value::Integer(v),
            ColValue::UnsignedLongLong(v) => mlua::Value::Integer(v as i64),
            ColValue::Year(v) => mlua::Value::Integer(v as i64),
            ColValue::Bit(v) => mlua::Value::Integer(v as i64),
            ColValue::Set(v) => mlua::Value::Integer(v as i64),
            ColValue::Enum(v) => mlua::Value::Integer(v as i64),

            ColValue::Float(v) => mlua::Value::Number(v as f64),
            ColValue::Double(v) => mlua::Value::Number(v),

            ColValue::Decimal(v)
            | ColValue::Time(v)
            | ColValue::Date(v)
            | ColValue::DateTime(v)
            | ColValue::Timestamp(v)
            | ColValue::String(v)
            | ColValue::Set2(v)
            | ColValue::Enum2(v)
            | ColValue::Json2(v) => v.into_lua(lua)?,

            ColValue::Json3(_)
            | ColValue::RawString(_)
            | ColValue::Blob(_)
            | ColValue::Json(_)
            | ColValue::MongoDoc(_)
            | ColValue::None
            | ColValue::UnchangedToast => mlua::Value::NULL,
        };
        Ok(lua_value)
    }

    fn lua_value_to_col_value(&self, lua_value: mlua::Value) -> anyhow::Result<ColValue> {
        let col_value = match lua_value {
            mlua::Value::Boolean(v) => ColValue::Bool(v),
            mlua::Value::Integer(v) => ColValue::LongLong(v),
            mlua::Value::Number(v) => ColValue::Double(v),
            mlua::Value::String(v) => ColValue::String(v.to_str()?.to_string()),
            _ => ColValue::None,
        };
        Ok(col_value)
    }
}
