package com.curso.aws.model;

import java.util.Set;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "libros")
public class Libro {
	private String isbn;
	private String titulo;
	private String autor;
	private String ignoroElCampo;
	private Set<String> categorias;

	@DynamoDBHashKey(attributeName = "isbn")
	public String getIsbn() {
		return isbn;
	}

	public void setIsbn(String isbn) {
		this.isbn = isbn;
	}

	@DynamoDBRangeKey(attributeName = "titulo")
	public String getTitulo() {
		return titulo;
	}

	public void setTitulo(String titulo) {
		this.titulo = titulo;
	}

	@DynamoDBAttribute(attributeName = "autor")
	public String getAutor() {
		return autor;
	}

	public void setAutor(String autor) {
		this.autor = autor;
	}

	@DynamoDBAttribute(attributeName = "categorias")
	public Set<String> getCategorias() {
		return categorias;
	}

	public void setCategorias(Set<String> categorias) {
		this.categorias = categorias;
	}

	@DynamoDBIgnore
	public String getIgnoroElCampo() {
		return ignoroElCampo;
	}

	public void setIgnoroElCampo(String ignoroElCampo) {
		this.ignoroElCampo = ignoroElCampo;
	}
}
