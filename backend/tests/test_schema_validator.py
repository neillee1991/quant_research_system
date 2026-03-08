"""
Schema Validator 单元测试
"""
import pytest
from app.validators.schema_validator import SchemaValidator


class TestSchemaValidator:
    """SchemaValidator 测试类"""

    def test_validate_valid_schema(self):
        """测试验证有效的 schema"""
        schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "trade_date": {
                "type": "DATE",
                "nullable": False,
                "comment": "交易日期"
            },
            "close": {
                "type": "DOUBLE",
                "nullable": True,
                "comment": "收盘价"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema(schema)
        assert is_valid
        assert len(errors) == 0

    def test_validate_schema_not_dict(self):
        """测试 schema 不是 dict"""
        is_valid, errors = SchemaValidator.validate_schema("not a dict")
        assert not is_valid
        assert "Schema must be a dict" in errors

    def test_validate_empty_schema(self):
        """测试空 schema"""
        is_valid, errors = SchemaValidator.validate_schema({})
        assert not is_valid
        assert "Schema cannot be empty" in errors

    def test_validate_field_missing_keys(self):
        """测试字段缺少必需的 key"""
        schema = {
            "ts_code": {
                "type": "SYMBOL"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema(schema)
        assert not is_valid
        assert any("missing required keys" in err for err in errors)

    def test_validate_invalid_type(self):
        """测试无效的 DolphinDB 类型"""
        schema = {
            "ts_code": {
                "type": "VARCHAR",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema(schema)
        assert not is_valid
        assert any("invalid type" in err for err in errors)

    def test_validate_nullable_not_bool(self):
        """测试 nullable 不是 bool"""
        schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": "false",
                "comment": "股票代码"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema(schema)
        assert not is_valid
        assert any("nullable must be bool" in err for err in errors)

    def test_validate_comment_not_str(self):
        """测试 comment 不是 str"""
        schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": 123
            }
        }

        is_valid, errors = SchemaValidator.validate_schema(schema)
        assert not is_valid
        assert any("comment must be str" in err for err in errors)

    def test_validate_primary_keys_valid(self):
        """测试主键验证 - 有效"""
        schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "trade_date": {
                "type": "DATE",
                "nullable": False,
                "comment": "交易日期"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema(
            schema,
            primary_keys=["ts_code", "trade_date"]
        )
        assert is_valid
        assert len(errors) == 0

    def test_validate_primary_keys_missing(self):
        """测试主键验证 - 主键不在 schema 中"""
        schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema(
            schema,
            primary_keys=["ts_code", "trade_date"]
        )
        assert not is_valid
        assert any("Primary key 'trade_date' not found" in err for err in errors)

    def test_validate_all_dolphindb_types(self):
        """测试所有支持的 DolphinDB 类型"""
        schema = {
            "bool_field": {"type": "BOOL", "nullable": False, "comment": "布尔"},
            "char_field": {"type": "CHAR", "nullable": False, "comment": "字符"},
            "short_field": {"type": "SHORT", "nullable": False, "comment": "短整型"},
            "int_field": {"type": "INT", "nullable": False, "comment": "整型"},
            "long_field": {"type": "LONG", "nullable": False, "comment": "长整型"},
            "float_field": {"type": "FLOAT", "nullable": False, "comment": "浮点"},
            "double_field": {"type": "DOUBLE", "nullable": False, "comment": "双精度"},
            "string_field": {"type": "STRING", "nullable": False, "comment": "字符串"},
            "symbol_field": {"type": "SYMBOL", "nullable": False, "comment": "符号"},
            "date_field": {"type": "DATE", "nullable": False, "comment": "日期"},
            "timestamp_field": {"type": "TIMESTAMP", "nullable": False, "comment": "时间戳"},
            "time_field": {"type": "TIME", "nullable": False, "comment": "时间"},
        }

        is_valid, errors = SchemaValidator.validate_schema(schema)
        assert is_valid
        assert len(errors) == 0


class TestCompareSchemas:
    """Schema 比较测试类"""

    def test_compare_identical_schemas(self):
        """测试相同的 schema"""
        schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        is_compatible, errors, changes = SchemaValidator.compare_schemas(schema, schema)
        assert is_compatible
        assert len(errors) == 0
        assert len(changes["added"]) == 0
        assert len(changes["removed"]) == 0

    def test_compare_add_field(self):
        """测试新增字段（允许）"""
        old_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        new_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "trade_date": {
                "type": "DATE",
                "nullable": False,
                "comment": "交易日期"
            }
        }

        is_compatible, errors, changes = SchemaValidator.compare_schemas(
            old_schema,
            new_schema
        )
        assert is_compatible
        assert len(errors) == 0
        assert "trade_date" in changes["added"]

    def test_compare_remove_field(self):
        """测试删除字段（禁止）"""
        old_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "trade_date": {
                "type": "DATE",
                "nullable": False,
                "comment": "交易日期"
            }
        }

        new_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        is_compatible, errors, changes = SchemaValidator.compare_schemas(
            old_schema,
            new_schema
        )
        assert not is_compatible
        assert any("Cannot remove fields" in err for err in errors)
        assert "trade_date" in changes["removed"]

    def test_compare_change_type(self):
        """测试修改字段类型（禁止）"""
        old_schema = {
            "close": {
                "type": "FLOAT",
                "nullable": False,
                "comment": "收盘价"
            }
        }

        new_schema = {
            "close": {
                "type": "DOUBLE",
                "nullable": False,
                "comment": "收盘价"
            }
        }

        is_compatible, errors, changes = SchemaValidator.compare_schemas(
            old_schema,
            new_schema
        )
        assert not is_compatible
        assert any("Cannot change type" in err for err in errors)
        assert len(changes["type_changed"]) == 1
        assert changes["type_changed"][0]["field"] == "close"
        assert changes["type_changed"][0]["old_type"] == "FLOAT"
        assert changes["type_changed"][0]["new_type"] == "DOUBLE"

    def test_compare_change_nullable(self):
        """测试修改 nullable（记录但不报错）"""
        old_schema = {
            "close": {
                "type": "DOUBLE",
                "nullable": False,
                "comment": "收盘价"
            }
        }

        new_schema = {
            "close": {
                "type": "DOUBLE",
                "nullable": True,
                "comment": "收盘价"
            }
        }

        is_compatible, errors, changes = SchemaValidator.compare_schemas(
            old_schema,
            new_schema
        )
        assert is_compatible
        assert len(errors) == 0
        assert len(changes["nullable_changed"]) == 1
        assert changes["nullable_changed"][0]["field"] == "close"

    def test_compare_multiple_changes(self):
        """测试多个变更"""
        old_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "close": {
                "type": "FLOAT",
                "nullable": False,
                "comment": "收盘价"
            },
            "volume": {
                "type": "LONG",
                "nullable": False,
                "comment": "成交量"
            }
        }

        new_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "close": {
                "type": "DOUBLE",
                "nullable": False,
                "comment": "收盘价"
            },
            "amount": {
                "type": "DOUBLE",
                "nullable": True,
                "comment": "成交额"
            }
        }

        is_compatible, errors, changes = SchemaValidator.compare_schemas(
            old_schema,
            new_schema
        )
        assert not is_compatible
        assert len(errors) == 2
        assert "volume" in changes["removed"]
        assert "amount" in changes["added"]
        assert len(changes["type_changed"]) == 1


class TestValidateSchemaEvolution:
    """Schema 演化验证测试类"""

    def test_validate_evolution_valid(self):
        """测试有效的 schema 演化"""
        old_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        new_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "trade_date": {
                "type": "DATE",
                "nullable": False,
                "comment": "交易日期"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema_evolution(
            old_schema,
            new_schema,
            primary_keys=["ts_code", "trade_date"]
        )
        assert is_valid
        assert len(errors) == 0

    def test_validate_evolution_invalid_new_schema(self):
        """测试新 schema 本身无效"""
        old_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        new_schema = {
            "ts_code": {
                "type": "INVALID_TYPE",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema_evolution(
            old_schema,
            new_schema
        )
        assert not is_valid
        assert any("invalid type" in err for err in errors)

    def test_validate_evolution_incompatible(self):
        """测试不兼容的 schema 演化"""
        old_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            },
            "close": {
                "type": "FLOAT",
                "nullable": False,
                "comment": "收盘价"
            }
        }

        new_schema = {
            "ts_code": {
                "type": "SYMBOL",
                "nullable": False,
                "comment": "股票代码"
            }
        }

        is_valid, errors = SchemaValidator.validate_schema_evolution(
            old_schema,
            new_schema
        )
        assert not is_valid
        assert any("Cannot remove fields" in err for err in errors)
