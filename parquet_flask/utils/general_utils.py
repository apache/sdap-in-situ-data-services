from jsonschema import validate, ValidationError, FormatChecker


class GeneralUtils:
    @staticmethod
    def is_json_valid(payload, schema):
        try:
            validate(instance=payload, schema=schema, format_checker=FormatChecker())
        except ValidationError as error:
            return False, str(error)
        return True, None
