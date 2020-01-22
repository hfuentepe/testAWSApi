package com.cloud.aws.api.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.curso.aws.model.Libro;

public class OperacionesConDynamoDB {

	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	static DynamoDB dynamoDB = new DynamoDB(client);

	public static void main(String[] args) throws IOException, InterruptedException {
		// Creación de tabla de libros
		// Creación del esquema de claves
		ArrayList<KeySchemaElement> esquemaClaves = new ArrayList<KeySchemaElement>();
		// Agregamos la clave de partición
		esquemaClaves.add(new KeySchemaElement().withAttributeName("isbn").withKeyType(KeyType.HASH));
		// Definimos los atributos de esa clave
		ArrayList<AttributeDefinition> defAtributos = new ArrayList<AttributeDefinition>();
		defAtributos.add(new AttributeDefinition().withAttributeName("isbn").withAttributeType("S"));
		// Si creamos clave de ordenación, lo indicamos y lo agregamos a la definición
		// de atributos
		esquemaClaves.add(new KeySchemaElement().withAttributeName("titulo").withKeyType(KeyType.RANGE));
		defAtributos.add(new AttributeDefinition().withAttributeName("titulo").withAttributeType("S"));
		// Creación de tabla
		CreateTableRequest request = new CreateTableRequest().withTableName("libros").withKeySchema(esquemaClaves)
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(5L));
		request.setAttributeDefinitions(defAtributos);
		Table table = dynamoDB.createTable(request);
		table.waitForActive();
		System.out.println("Tabla creada");
		// Agregamos datos a la tabla
		Item item = new Item().withPrimaryKey("isbn", "123-456789")
				.withString("titulo", "El Señor de los anillos. Volumen I. La comunidad del anillo")
				.withString("autor", "J.R.R. Tolkien").withNumber("paginas", 500).withBoolean("publicado", true)
				.withStringSet("categorias", new HashSet<String>(Arrays.asList("Fantastico", "Novela")));
		table.putItem(item);
		item = new Item().withPrimaryKey("isbn", "123-456790")
				.withString("titulo", "El Señor de los anillos. Volumen II. Las dos torres.")
				.withString("autor", "J.R.R. Tolkien").withBoolean("publicado", false)
				.withStringSet("categorias", new HashSet<String>(Arrays.asList("Fantastico", "Novela")));
		table.putItem(item);
		item = new Item().withPrimaryKey("isbn", "123-456791")
				.withString("titulo", "El Señor de los anillos. Volumen III. El retorno del Rey")
				.withString("autor", "J.R.R. Tolkien").withNumber("paginas", 500).withBoolean("publicado", true)
				.withStringSet("categorias", new HashSet<String>(Arrays.asList("Fantastico", "Novela")));
		table.putItem(item);
		// Consulta de todos los datos
		ScanRequest scanRequest = new ScanRequest().withTableName("libros");
		ScanResult result = client.scan(scanRequest);
		for (Map<String, AttributeValue> itemResult : result.getItems()) {
			for (String key : itemResult.keySet()) {
				System.out.println("key: " + key + " - Value: " + itemResult.get(key));
			}
		}
		// Consulta de datos con condicion y proyeccion
		Map<String, AttributeValue> valores = new HashMap<String, AttributeValue>();
		valores.put(":autor", new AttributeValue().withS("J.R.R. Tolkien"));
		scanRequest = new ScanRequest().withTableName("libros").withFilterExpression("autor = :autor")
				.withExpressionAttributeValues(valores).withProjectionExpression("titulo,autor");
		result = client.scan(scanRequest);
		for (Map<String, AttributeValue> itemResult : result.getItems()) {
			for (String key : itemResult.keySet()) {
				System.out.println("key: " + key + " - Value: " + itemResult.get(key));
			}
		}
		// Consulta de un dato
		QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("isbn = :v_isbn")
				.withValueMap(new ValueMap().withString(":v_isbn", "123-456791"));
		ItemCollection<QueryOutcome> items = table.query(querySpec);
		for (Item itemResult : items) {
			System.out.println(itemResult.toJSONPretty());
		}
		// Actualización
		UpdateItemSpec updateItemSpec = new UpdateItemSpec()
				.withPrimaryKey("isbn", "123-456791", "titulo",
						"El Señor de los anillos. Volumen III. El retorno del Rey")
				.withUpdateExpression("set paginas = :paginas").withValueMap(new ValueMap().withNumber(":paginas", 337))
				.withReturnValues(ReturnValue.UPDATED_NEW);
		table.updateItem(updateItemSpec);
		querySpec = new QuerySpec().withKeyConditionExpression("isbn = :v_isbn")
				.withValueMap(new ValueMap().withString(":v_isbn", "123-456791"))
				.withProjectionExpression("isbn,titulo,paginas");
		items = table.query(querySpec);
		for (Item itemResult : items) {
			System.out.println(itemResult.toJSONPretty());
		}
		
		// Operaciones de guardado con modelo asociado
		DynamoDBMapper mapper = new DynamoDBMapper(client);
		Libro libro = new Libro();
		libro.setIsbn("1389712371");
		;
		libro.setTitulo("Toma Libro");
		libro.setAutor("Toma autor");
		libro.setCategorias(new HashSet<String>(Arrays.asList("Terror", "Comedia")));
		libro.setIgnoroElCampo("Campo ignorado");
		mapper.save(libro);
		
		// Operaciones de consulta con modelo asociado
		Libro otro = new Libro();
		otro.setIsbn("123-456789");
		DynamoDBQueryExpression<Libro> queryExpression = new DynamoDBQueryExpression<Libro>().withHashKeyValues(otro);
		List<Libro> itemList = mapper.query(Libro.class, queryExpression);
		for (int i = 0; i < itemList.size(); i++) {
			System.out.println(itemList.get(i).getTitulo());
			System.out.println(itemList.get(i).getIgnoroElCampo());
		}
		
		// Eliminación de item
		DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
				.withPrimaryKey("isbn", "1389712371", "titulo", "Toma Libro").withReturnValues(ReturnValue.ALL_OLD);
		table.deleteItem(deleteItemSpec);
		// Borrado de tabla libros
		table.delete();
		table.waitForDelete();
		System.out.println("Tabla borrada");
	}
}