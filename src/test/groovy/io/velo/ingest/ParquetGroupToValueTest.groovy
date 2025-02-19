package io.velo.ingest

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.schema.MessageTypeParser
import spock.lang.Specification

class ParquetGroupToValueTest extends Specification {
    def 'test all'() {
        given:
        def schemaString = '''
            message example {
              required int32 id;
              required binary name (UTF8);
              required int64 user_id;
              required float score0;
              required double score;
              required boolean flag;
              required binary des (UTF8);
            }
        '''
        def schema = MessageTypeParser.parseMessageType(schemaString)

        and:
        def map = [
                id     : 1,
                name   : 'John',
                user_id: 100L,
                score0 : 9.5f,
                score  : 9.5d,
                flag   : true,
                des    : 'abc xyz ooo 111 222 333'
        ]
        def groupFactory = new SimpleGroupFactory(schema)
        def group = groupFactory.newGroup()
        group.append('id', map.id as int)
        group.append('name', map.name as String)
        group.append('user_id', map.user_id as long)
        group.append('score0', map.score0 as float)
        group.append('score', map.score as double)
        group.append('flag', map.flag as boolean)
        group.append('des', map.des as String)

        when:
        def objectMapper = new ObjectMapper()
        def string = ParquetGroupToValue.toJson(schema, objectMapper, group)
        println string
        def toMap = objectMapper.readValue(string, Map)
        then:
        toMap == map

        when:
        def string2 = ParquetGroupToValue.toCsv(schema, group)
        then:
        string2 == '1,John,100,9.5,9.5,true,abc xyz ooo 111 222 333'
    }
}
