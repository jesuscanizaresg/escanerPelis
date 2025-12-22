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
        String sql = """
                CREATE TABLE IF NOT EXISTS peliculas (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 titulo TEXT NOT NULL,
                 titulo_base TEXT NOT NULL,
                 anio INTEGER,
                 version TEXT,
                 extension TEXT NOT NULL,
                 tamano INTEGER NOT NULL,
                 fecha_modificacion TEXT NOT NULL,
                 ruta TEXT NOT NULL
                );""";

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            ensureColumnExists(conn, "version", "ALTER TABLE peliculas ADD COLUMN version TEXT;");
            ensureColumnExists(conn, "ruta", "ALTER TABLE peliculas ADD COLUMN ruta TEXT NOT NULL DEFAULT '';");
            ensureColumnExists(conn, "titulo_base", "ALTER TABLE peliculas ADD COLUMN titulo_base TEXT NOT NULL DEFAULT '';");

            migrarTitulosBase(conn);
            // Aseguramos unicidad por titulo base eliminando duplicados previos antes de crear el indice unico.
            limpiarTitulosDuplicados(conn);

            stmt.execute("DROP INDEX IF EXISTS idx_peliculas_titulo;");
            stmt.execute("DROP INDEX IF EXISTS idx_peliculas_ruta;");
            stmt.execute("DROP INDEX IF EXISTS idx_peliculas_titulo_anio_version;");
            stmt.execute("DROP INDEX IF EXISTS idx_peliculas_titulo_base;");

            stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_peliculas_titulo_base ON peliculas(titulo_base);");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_peliculas_titulo_base_anio_version ON peliculas(titulo_base, anio, version);");
            System.out.println("Tabla 'peliculas' creada o actualizada exitosamente.");
        } catch (SQLException e) {
            System.out.println("Error al crear o actualizar la tabla 'peliculas': " + e.getMessage());
        }
    }

    public static void insertarPelicula(String titulo, String tituloBase, int anio, String version, String extension, long tamano,
                                        String fechaModificacion, String ruta) {
        String sql = "INSERT INTO peliculas(titulo, titulo_base, anio, version, extension, tamano, fecha_modificacion, ruta) VALUES(?,?,?,?,?,?,?,?)";
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, titulo);
            pstmt.setString(2, tituloBase);
            pstmt.setInt(3, anio);
            pstmt.setString(4, version);
            pstmt.setString(5, extension);
            pstmt.setLong(6, tamano);
            pstmt.setString(7, fechaModificacion);
            pstmt.setString(8, ruta);
            pstmt.executeUpdate();
            System.out.println("Pelicula insertada: " + titulo);
        } catch (SQLException e) {
            System.out.println("Error al insertar pelicula en la base de datos: " + e.getMessage());
        }
    }

    public static boolean existePeliculaPorTituloBase(String tituloBase) {
        String sql = "SELECT id FROM peliculas WHERE titulo_base = ?";
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, tituloBase);
            ResultSet rs = pstmt.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            System.out.println("Error al verificar la existencia de la pelicula por titulo base: " + e.getMessage());
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

    private static void migrarTitulosBase(Connection conn) throws SQLException {
        String updateSql = """
                UPDATE peliculas
                SET titulo_base = CASE
                    WHEN (titulo_base IS NOT NULL AND LENGTH(TRIM(titulo_base)) > 0) THEN titulo_base
                    WHEN (extension IS NULL OR LENGTH(TRIM(extension)) = 0 OR INSTR(titulo, '.') = 0) THEN titulo
                    ELSE SUBSTR(titulo, 1, LENGTH(titulo) - LENGTH(extension) - 1)
                END
                WHERE titulo IS NOT NULL;
                """;
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(updateSql);
        }
    }

    private static void limpiarTitulosDuplicados(Connection conn) throws SQLException {
        String deletePorBase = "DELETE FROM peliculas "
                + "WHERE titulo_base IN (SELECT titulo_base FROM peliculas GROUP BY titulo_base HAVING COUNT(*) > 1) "
                + "AND id NOT IN (SELECT MIN(id) FROM peliculas GROUP BY titulo_base);";
        String deletePorTitulo = "DELETE FROM peliculas "
                + "WHERE titulo IN (SELECT titulo FROM peliculas GROUP BY titulo HAVING COUNT(*) > 1) "
                + "AND id NOT IN (SELECT MIN(id) FROM peliculas GROUP BY titulo);";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(deletePorBase);
            stmt.executeUpdate(deletePorTitulo);
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
