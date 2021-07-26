import fastjsonschema


class GeneralUtils:
    @staticmethod
    def is_json_valid(payload, schema):
        try:
            fastjsonschema.validate(schema, payload)
        except Exception as error:
            return False, str(error)
        return True, None

    @staticmethod
    def chunk_list(input_list, chunked_size):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(input_list), chunked_size):
            yield input_list[i:i + chunked_size]
