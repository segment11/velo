import com.fasterxml.jackson.databind.ObjectMapper
import io.velo.ingest.ParquetGroupToValue
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.schema.MessageTypeParser

def schemaString = '''
    message example {
      required int32 id;
      required binary name (UTF8);
      required double score;
      required binary des (UTF8);
    }
'''
def schema = MessageTypeParser.parseMessageType(schemaString)

def path = new Path('sample.parquet')

// write
def writer = ExampleParquetWriter.builder(path)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(schema)
        .build()

def groupFactory = new SimpleGroupFactory(schema)

def longDes = 'abc xyz ooo 111 222 333' * 10
100.times {
    def alice = groupFactory.newGroup()
            .append('id', 100 + it)
            .append('name', 'Alice ' + it)
            .append('score', 9.5)
            .append('des', longDes)
    writer.write(alice)

    def bob = groupFactory.newGroup()
            .append('id', 200 + it)
            .append('name', 'Bob ' + it)
            .append('score', 8.5)
            .append('des', longDes)
    writer.write(bob)

    def charlie = groupFactory.newGroup()
            .append('id', 300 + it)
            .append('name', 'Charlie ' + it)
            .append('score', 7.5)
            .append('des', longDes)
    writer.write(charlie)
}

writer.close()

// read
def readSupport = new GroupReadSupport()
ParquetReader.Builder<Group> readerBuilder = ParquetReader.builder(readSupport, path)
ParquetReader<Group> reader = readerBuilder.build()

def objectMapper = new ObjectMapper()
Group group
while ((group = reader.read()) != null) {
//    int id = group.getInteger('id', 0)
//    def name = group.getString('name', 0)
//    double score = group.getDouble('score', 0)
//    println("id: $id, name: $name, score: $score")

    println ParquetGroupToValue.toJson(schema, objectMapper, group)
//    println ParquetGroupToValue.toCsv(schema, group)
}

reader.close()

