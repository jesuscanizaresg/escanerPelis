package com.peliculas.database.escaneadorPeliculas;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {

	private static final String DEFAULT_DB_FILENAME = "peliculas.db";
	private static String dbFilename = DEFAULT_DB_FILENAME;

	public static Connection connect() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(getUrl());
		} catch (SQLException e) {
			System.out.println("Error al conectar a la base de datos: " + e.getMessage());
		}
		return conn;
	}

	public static void createOrUpdateTable() {
		String sql = "CREATE TABLE IF NOT EXISTS peliculas (\n"
				+ " id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
				+ " titulo TEXT NOT NULL,\n"
				+ " anio INTEGER,\n"
				+ " version TEXT,\n"
				+ " extension TEXT NOT NULL,\n"
				+ " tamano INTEGER NOT NULL,\n"
				+ " fecha_modificacion TEXT NOT NULL,\n"
				+ " ruta TEXT NOT NULL\n"
				+ ");";

		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
			ensureColumnExists(conn, "version", "ALTER TABLE peliculas ADD COLUMN version TEXT;");
			ensureColumnExists(conn, "ruta", "ALTER TABLE peliculas ADD COLUMN ruta TEXT NOT NULL DEFAULT '';");
			// Aseguramos unicidad por ruta eliminando duplicados previos antes de crear el indice unico.
			limpiarRutasDuplicadas(conn);
			// Aseguramos unicidad por titulo eliminando duplicados previos antes de crear el indice unico.
			limpiarTitulosDuplicados(conn);
			stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peliculas_ruta ON peliculas(ruta);");
			stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peliculas_titulo ON peliculas(titulo);");
			stmt.execute("CREATE INDEX IF NOT EXISTS idx_peliculas_titulo_anio_version ON peliculas(titulo, anio, version);");
			System.out.println("Tabla 'peliculas' creada o actualizada exitosamente.");
		} catch (SQLException e) {
			System.out.println("Error al crear o actualizar la tabla 'peliculas': " + e.getMessage());
		}
	}

	public static boolean insertarPelicula(String titulo, int anio, String version, String extension, long tamano,
										   String fechaModificacion, String ruta) {
		String sql = "INSERT INTO peliculas(titulo, anio, version, extension, tamano, fecha_modificacion, ruta) VALUES(?,?,?,?,?,?,?)";
		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setString(1, titulo);
			pstmt.setInt(2, anio);
			pstmt.setString(3, version);
			pstmt.setString(4, extension);
			pstmt.setLong(5, tamano);
			pstmt.setString(6, fechaModificacion);
			pstmt.setString(7, ruta);
			pstmt.executeUpdate();
			System.out.println("Pelicula insertada correctamente: " + titulo + " (" + anio + ") " + version);
			return true;
		} catch (SQLException e) {
			System.out.println("Error al insertar pelicula en la base de datos: " + e.getMessage());
			return false;
		}
	}

	public static boolean existePelicula(String titulo, int anio, String version) {
		String sql = "SELECT id FROM peliculas WHERE titulo = ? AND anio = ? AND version = ?";
		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setString(1, titulo);
			pstmt.setInt(2, anio);
			pstmt.setString(3, version);
			ResultSet rs = pstmt.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			System.out.println("Error al verificar la existencia de la pelicula: " + e.getMessage());
			return false;
		}
	}

	public static boolean existePeliculaPorRuta(String ruta) {
		String sql = "SELECT id FROM peliculas WHERE ruta = ?";
		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setString(1, ruta);
			ResultSet rs = pstmt.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			System.out.println("Error al verificar la existencia de la pelicula por ruta: " + e.getMessage());
			return false;
		}
	}

	public static boolean existePeliculaPorTitulo(String titulo) {
		String sql = "SELECT id FROM peliculas WHERE titulo = ?";
		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setString(1, titulo);
			ResultSet rs = pstmt.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			System.out.println("Error al verificar la existencia de la pelicula por titulo: " + e.getMessage());
			return false;
		}
	}

	public static void borrarBaseDatos() {
		File dbFile = new File(dbFilename);
		if (dbFile.exists()) {
			if (dbFile.delete()) {
				System.out.println("Base de datos borrada exitosamente.");
			} else {
				System.out.println("No se pudo borrar la base de datos.");
			}
		} else {
			System.out.println("La base de datos no existe.");
		}
	}

	private static void ensureColumnExists(Connection conn, String columnName, String alterStatement) throws SQLException {
		String checkSql = "PRAGMA table_info(peliculas)";
		try (PreparedStatement pstmt = conn.prepareStatement(checkSql); ResultSet rs = pstmt.executeQuery()) {
			while (rs.next()) {
				String existingColumn = rs.getString("name");
				if (existingColumn != null && existingColumn.equalsIgnoreCase(columnName)) {
					return;
				}
			}
		}
		try (Statement stmt = conn.createStatement()) {
			stmt.execute(alterStatement);
		}
	}

	private static void limpiarRutasDuplicadas(Connection conn) throws SQLException {
		String deleteSql = "DELETE FROM peliculas "
				+ "WHERE ruta IN (SELECT ruta FROM peliculas GROUP BY ruta HAVING COUNT(*) > 1) "
				+ "AND id NOT IN (SELECT MIN(id) FROM peliculas GROUP BY ruta);";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(deleteSql);
		}
	}

	private static void limpiarTitulosDuplicados(Connection conn) throws SQLException {
		String deleteSql = "DELETE FROM peliculas "
				+ "WHERE titulo IN (SELECT titulo FROM peliculas GROUP BY titulo HAVING COUNT(*) > 1) "
				+ "AND id NOT IN (SELECT MIN(id) FROM peliculas GROUP BY titulo);";
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(deleteSql);
		}
	}

	public static void setDatabaseFilename(String filename) {
		if (filename == null || filename.trim().isEmpty()) {
			dbFilename = DEFAULT_DB_FILENAME;
			return;
		}
		dbFilename = filename.trim();
	}

	public static String getDatabaseFilename() {
		return dbFilename;
	}

	public static String getDefaultDatabaseFilename() {
		return DEFAULT_DB_FILENAME;
	}

	private static String getUrl() {
		return "jdbc:sqlite:" + dbFilename;
	}
}
