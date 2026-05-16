//! SQL parser integration using the `sqlparser` crate (v0.44).

use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::crdt::merge::{ColumnDef, ColumnType, ForeignKeyDef, TableSchema};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),
    #[error("Unsupported statement: {0}")]
    Unsupported(String),
    #[error("Invalid SQL: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub enum ParsedStatement {
    CreateTable { schema: TableSchema },
    CreateIndex { index_name: String, table_name: String, columns: Vec<String> },
    Insert { table: String, columns: Vec<String>, values: Vec<Vec<SqlValue>> },
    Update { table: String, assignments: Vec<(String, SqlValue)>, filter: Option<SqlExpr> },
    Delete { table: String, filter: Option<SqlExpr> },
    Select { table: String, columns: SelectColumns, filter: Option<SqlExpr>, order_by: Vec<(String, bool)> },
}

#[derive(Debug, Clone)]
pub enum SelectColumns { All, Named(Vec<String>) }

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Placeholder(usize),
}

impl SqlValue {
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            SqlValue::Null => None,
            SqlValue::Text(s) => Some(s.as_bytes().to_vec()),
            SqlValue::Integer(i) => Some(i.to_le_bytes().to_vec()),
            SqlValue::Float(f) => Some(f.to_bits().to_le_bytes().to_vec()),
            SqlValue::Boolean(b) => Some(vec![if *b { 1 } else { 0 }]),
            SqlValue::Placeholder(_) => None,
        }
    }

    pub fn to_bytes_with_params(&self, params: &[SqlValue]) -> Option<Vec<u8>> {
        if let SqlValue::Placeholder(i) = self {
            params.get(*i).and_then(|v| v.to_bytes())
        } else {
            self.to_bytes()
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlExpr {
    Eq { column: String, value: SqlValue },
    Neq { column: String, value: SqlValue },
    Lt { column: String, value: SqlValue },
    Gt { column: String, value: SqlValue },
    And(Box<SqlExpr>, Box<SqlExpr>),
    Or(Box<SqlExpr>, Box<SqlExpr>),
    IsNull { column: String },
    IsNotNull { column: String },
}

impl SqlExpr {
    pub fn evaluate<F>(&self, read_cell: &F) -> bool
    where F: Fn(&str) -> Option<Vec<u8>> {
        match self {
            SqlExpr::Eq { column, value } =>
                read_cell(column).as_deref() == value.to_bytes().as_deref(),
            SqlExpr::Neq { column, value } =>
                read_cell(column).as_deref() != value.to_bytes().as_deref(),
            SqlExpr::IsNull { column } => read_cell(column).is_none(),
            SqlExpr::IsNotNull { column } => read_cell(column).is_some(),
            SqlExpr::And(l, r) => l.evaluate(read_cell) && r.evaluate(read_cell),
            SqlExpr::Or(l, r) => l.evaluate(read_cell) || r.evaluate(read_cell),
            SqlExpr::Lt { column, value } =>
                read_cell(column).as_deref().cmp(&value.to_bytes().as_deref()) == std::cmp::Ordering::Less,
            SqlExpr::Gt { column, value } =>
                read_cell(column).as_deref().cmp(&value.to_bytes().as_deref()) == std::cmp::Ordering::Greater,
        }
    }
}

pub fn parse_sql(sql: &str) -> Result<Vec<ParsedStatement>, ParseError> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)?;
    stmts.into_iter().map(convert_statement).collect()
}

fn convert_statement(stmt: Statement) -> Result<ParsedStatement, ParseError> {
    match stmt {
        // sqlparser 0.44: CreateTable is a struct-style variant
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            let table_name = name.to_string();
            let mut col_defs = Vec::new();
            let mut foreign_keys = Vec::new();

            for col_def in &columns {
                let col_name = col_def.name.value.clone();
                let col_type = map_data_type(&col_def.data_type);
                let mut not_null = false;
                let mut primary_key = false;
                let mut unique = false;
                let mut default_value: Option<Vec<u8>> = None;

                for opt in &col_def.options {
                    match &opt.option {
                        ColumnOption::NotNull => not_null = true,
                        ColumnOption::Unique { is_primary, .. } => {
                            if *is_primary { primary_key = true; not_null = true; }
                            else { unique = true; }
                        }
                        ColumnOption::Default(expr) => {
                            if let Expr::Value(val) = expr {
                                default_value = convert_value(val).to_bytes();
                            }
                        }
                        ColumnOption::ForeignKey { foreign_table, referred_columns, .. } => {
                            let ref_col = referred_columns.first()
                                .map(|c| c.value.clone())
                                .unwrap_or_else(|| "id".to_string());
                            foreign_keys.push(ForeignKeyDef {
                                column: col_name.clone(),
                                ref_table: foreign_table.to_string(),
                                ref_column: ref_col,
                            });
                        }
                        _ => {}
                    }
                }
                col_defs.push(ColumnDef { name: col_name, col_type, not_null, default_value, primary_key, unique });
            }

            // Table-level FK and UNIQUE constraints
            let mut composite_unique_constraints: Vec<Vec<String>> = Vec::new();
            for constraint in &constraints {
                match constraint {
                    TableConstraint::ForeignKey { columns: fk_cols, foreign_table, referred_columns, .. } => {
                        let col = fk_cols.first().map(|c| c.value.clone()).unwrap_or_default();
                        let ref_col = referred_columns.first().map(|c| c.value.clone()).unwrap_or_else(|| "id".into());
                        foreign_keys.push(ForeignKeyDef {
                            column: col,
                            ref_table: foreign_table.to_string(),
                            ref_column: ref_col,
                        });
                    }
                    TableConstraint::Unique { columns, .. } => {
                        let cols: Vec<String> = columns.iter()
                            .map(|c| c.value.clone())
                            .collect();
                        if cols.len() == 1 {
                            // Single-column table-level UNIQUE — mark the column def as unique
                            if let Some(col_def) = col_defs.iter_mut().find(|c| c.name == cols[0]) {
                                col_def.unique = true;
                            }
                        } else if cols.len() > 1 {
                            // Composite UNIQUE constraint
                            composite_unique_constraints.push(cols);
                        }
                    }
                    _ => {}
                }
            }

            Ok(ParsedStatement::CreateTable {
                schema: TableSchema {
                    name: table_name,
                    columns: col_defs,
                    foreign_keys,
                    indexes: vec![],
                    composite_unique_constraints,
                },
            })
        }

        // sqlparser 0.44: CreateIndex is a struct-style variant
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            ..
        } => {
            let index_name = name.map(|n| n.to_string()).unwrap_or_default();
            let tname = table_name.to_string();
            let cols = columns.iter()
                .map(|c| match &c.expr {
                    Expr::Identifier(ident) => ident.value.clone(),
                    other => format!("{}", other),
                })
                .collect();
            Ok(ParsedStatement::CreateIndex { index_name, table_name: tname, columns: cols })
        }

        // sqlparser 0.44: Insert is a struct-style variant
        Statement::Insert { table_name, columns, source, .. } => {
            let table = table_name.to_string();
            let cols = columns.iter().map(|c| c.value.clone()).collect();
            let mut value_rows = Vec::new();
            if let Some(source) = source {
                if let SetExpr::Values(vals) = *source.body {
                    for row in vals.rows {
                        let converted: Vec<SqlValue> = row.iter().map(convert_expr).collect();
                        value_rows.push(converted);
                    }
                }
            }
            Ok(ParsedStatement::Insert { table, columns: cols, values: value_rows })
        }

        Statement::Update { table, assignments, selection, .. } => {
            let table_name = match table.relation {
                TableFactor::Table { name, .. } => name.to_string(),
                other => return Err(ParseError::Unsupported(format!("table factor: {:?}", other))),
            };
            // sqlparser 0.44: Assignment has `id: Vec<Ident>` and `value: Expr`
            let assigns: Vec<(String, SqlValue)> = assignments.iter()
                .map(|a| {
                    let col = a.id.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                    let val = convert_expr(&a.value);
                    (col, val)
                })
                .collect();
            let filter = selection.map(|e| convert_expr_to_sql_expr(&e));
            Ok(ParsedStatement::Update { table: table_name, assignments: assigns, filter })
        }

        // sqlparser 0.44: Delete is a struct-style variant
        Statement::Delete { from, selection, .. } => {
            // In sqlparser 0.44, `from` is a `FromTable` enum
            let tables_ref: &[TableWithJoins] = match &from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
            };
            let table = tables_ref.first()
                .and_then(|f| {
                    if let TableFactor::Table { name, .. } = &f.relation {
                        Some(name.to_string())
                    } else { None }
                })
                .ok_or_else(|| ParseError::Invalid("DELETE missing table".into()))?;
            let filter = selection.map(|e| convert_expr_to_sql_expr(&e));
            Ok(ParsedStatement::Delete { table, filter })
        }

        Statement::Query(query) => {
            if let SetExpr::Select(select) = *query.body {
                let table = select.from.first()
                    .and_then(|f| {
                        if let TableFactor::Table { name, .. } = &f.relation {
                            Some(name.to_string())
                        } else { None }
                    })
                    .ok_or_else(|| ParseError::Invalid("SELECT missing FROM".into()))?;

                let columns = if select.projection.iter().any(|p| matches!(p, SelectItem::Wildcard(_))) {
                    SelectColumns::All
                } else {
                    let cols = select.projection.iter().map(|p| match p {
                        SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.clone(),
                        SelectItem::ExprWithAlias { expr: Expr::Identifier(id), .. } => id.value.clone(),
                        other => format!("{}", other),
                    }).collect();
                    SelectColumns::Named(cols)
                };

                let filter = select.selection.map(|e| convert_expr_to_sql_expr(&e));
                let order_by = query.order_by.iter()
                    .filter_map(|o| {
                        if let Expr::Identifier(id) = &o.expr {
                            Some((id.value.clone(), o.asc.unwrap_or(true)))
                        } else { None }
                    })
                    .collect();

                Ok(ParsedStatement::Select { table, columns, filter, order_by })
            } else {
                Err(ParseError::Unsupported("Non-SELECT query".into()))
            }
        }

        other => Err(ParseError::Unsupported(format!("{}", other))),
    }
}

fn map_data_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Text | DataType::Varchar(_) | DataType::Char(_) | DataType::CharVarying(_) => ColumnType::Text,
        DataType::Int(_) | DataType::Integer(_) | DataType::BigInt(_) | DataType::SmallInt(_) => ColumnType::Integer,
        DataType::Float(_) | DataType::Double | DataType::Real => ColumnType::Real,
        DataType::Boolean => ColumnType::Boolean,
        DataType::Blob(_) | DataType::Bytea => ColumnType::Blob,
        _ => ColumnType::Text,
    }
}

fn convert_value(val: &Value) -> SqlValue {
    match val {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => SqlValue::Text(s.clone()),
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() { SqlValue::Integer(i) }
            else if let Ok(f) = n.parse::<f64>() { SqlValue::Float(f) }
            else { SqlValue::Text(n.clone()) }
        }
        Value::Boolean(b) => SqlValue::Boolean(*b),
        Value::Null => SqlValue::Null,
        Value::Placeholder(p) => {
            let idx = p.trim_start_matches('?').trim_start_matches('$')
                .parse::<usize>().unwrap_or(0);
            SqlValue::Placeholder(idx.saturating_sub(1))
        }
        other => SqlValue::Text(format!("{}", other)),
    }
}

fn convert_expr(expr: &Expr) -> SqlValue {
    match expr {
        Expr::Value(v) => convert_value(v),
        Expr::Identifier(id) => SqlValue::Text(id.value.clone()),
        _ => SqlValue::Null,
    }
}

fn convert_expr_to_sql_expr(expr: &Expr) -> SqlExpr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match (left.as_ref(), op, right.as_ref()) {
                (Expr::Identifier(col), BinaryOperator::Eq, Expr::Value(val)) =>
                    SqlExpr::Eq { column: col.value.clone(), value: convert_value(val) },
                (Expr::Identifier(col), BinaryOperator::NotEq, Expr::Value(val)) =>
                    SqlExpr::Neq { column: col.value.clone(), value: convert_value(val) },
                (Expr::Identifier(col), BinaryOperator::Lt, Expr::Value(val)) =>
                    SqlExpr::Lt { column: col.value.clone(), value: convert_value(val) },
                (Expr::Identifier(col), BinaryOperator::Gt, Expr::Value(val)) =>
                    SqlExpr::Gt { column: col.value.clone(), value: convert_value(val) },
                (l, BinaryOperator::And, r) =>
                    SqlExpr::And(Box::new(convert_expr_to_sql_expr(l)), Box::new(convert_expr_to_sql_expr(r))),
                (l, BinaryOperator::Or, r) =>
                    SqlExpr::Or(Box::new(convert_expr_to_sql_expr(l)), Box::new(convert_expr_to_sql_expr(r))),
                _ => SqlExpr::Eq { column: "1".into(), value: SqlValue::Integer(1) },
            }
        }
        Expr::IsNull(inner) => {
            if let Expr::Identifier(col) = inner.as_ref() {
                SqlExpr::IsNull { column: col.value.clone() }
            } else {
                SqlExpr::IsNull { column: format!("{}", inner) }
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Identifier(col) = inner.as_ref() {
                SqlExpr::IsNotNull { column: col.value.clone() }
            } else {
                SqlExpr::IsNotNull { column: format!("{}", inner) }
            }
        }
        _ => SqlExpr::Eq { column: "1".into(), value: SqlValue::Integer(1) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table() {
        let sql = "CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT NOT NULL, name TEXT)";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        if let ParsedStatement::CreateTable { schema } = &stmts[0] {
            assert_eq!(schema.name, "users");
            assert_eq!(schema.columns.len(), 3);
            assert!(schema.columns[0].primary_key);
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn parse_insert() {
        let sql = "INSERT INTO users VALUES ('u1', 'alice@x.com', 'Alice')";
        let stmts = parse_sql(sql).unwrap();
        if let ParsedStatement::Insert { table, values, .. } = &stmts[0] {
            assert_eq!(table, "users");
            assert_eq!(values.len(), 1);
        }
    }

    #[test]
    fn parse_select_with_where() {
        let sql = "SELECT * FROM users WHERE id = 'u1'";
        let stmts = parse_sql(sql).unwrap();
        if let ParsedStatement::Select { table, filter, .. } = &stmts[0] {
            assert_eq!(table, "users");
            assert!(filter.is_some());
        }
    }

    #[test]
    fn parse_update() {
        let sql = "UPDATE users SET name = 'Alice Cooper' WHERE id = 'u1'";
        let stmts = parse_sql(sql).unwrap();
        assert!(matches!(&stmts[0], ParsedStatement::Update { .. }));
    }

    #[test]
    fn parse_delete() {
        let sql = "DELETE FROM users WHERE id = 'u1'";
        let stmts = parse_sql(sql).unwrap();
        assert!(matches!(&stmts[0], ParsedStatement::Delete { .. }));
    }
}
