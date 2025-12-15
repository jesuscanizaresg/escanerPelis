package com.peliculas.database.escaneadorPeliculas;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SpringBootApplication
public class EscaneadorPeliculas {

    private static final boolean borrarBaseDatos = false;
    private static final String directorioInformes = ".";
    private static final List<DuplicateEntry> archivosDuplicados = new ArrayList<>();
    private static String rutaRaizSeleccionada;
    private static final Scanner CONSOLE = new Scanner(System.in);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(EscaneadorPeliculas.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        ConfigurableApplicationContext context = application.run(args);

        System.out.println("Inicio de escaneo de peliculas.");

        borrarInformeDuplicados();

        DriveOption unidadSeleccionada = solicitarUnidadParaEscaneo();
        if (unidadSeleccionada == null) {
            System.out.println("No se selecciono ninguna unidad. Proceso cancelado.");
            SpringApplication.exit(context, () -> 0);
            return;
        }

        String nombreBaseDatos = solicitarNombreBaseDatos(DatabaseManager.getDefaultDatabaseFilename());
        DatabaseManager.setDatabaseFilename(nombreBaseDatos);
        System.out.println("Usando base de datos: " + DatabaseManager.getDatabaseFilename());

        if (borrarBaseDatos) {
            DatabaseManager.borrarBaseDatos();
            System.out.println("Base de datos borrada.");
        }

        DatabaseManager.createOrUpdateTable();

        String rutaBase = unidadSeleccionada.rutaParaEscanear().getAbsolutePath();
        rutaRaizSeleccionada = rutaBase;
        System.out.println("Escaneando en: " + rutaBase);
        FileScanner.scanDirectory(rutaBase);

        if (!archivosDuplicados.isEmpty()) {
            System.out.println("Existen duplicados, revisar informe duplicados.");
            generarInformeDuplicados(archivosDuplicados);
        } else {
            System.out.println("No se encontraron archivos duplicados.");
        }

        System.out.println("Escaneo completado. Fin del proceso.");
        SpringApplication.exit(context, () -> 0);
    }

    private static void borrarInformeDuplicados() {
        try {
            Files.walk(Paths.get(directorioInformes))
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith("InformeDuplicados_"))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            System.out.println("Informe de duplicados eliminado: " + path.getFileName());
                        } catch (IOException e) {
                            System.out.println("No se pudo eliminar el informe de duplicados: " + path.getFileName());
                        }
                    });
        } catch (IOException e) {
            System.out.println("Error al buscar archivos de informe de duplicados: " + e.getMessage());
        }
    }

    public static void procesarPelicula(File archivo) {
        String nombreArchivo = archivo.getName();
        String fechaModificacion = obtenerFechaModificacion(archivo);
        String extension = obtenerExtension(archivo);
        long tamano = archivo.length();
        int anio = obtenerAnio(archivo);
        String version = obtenerVersion(archivo);
        String origen = rutaRaizSeleccionada;

        if (DatabaseManager.existePeliculaPorTitulo(nombreArchivo)) {
            registrarDuplicado(nombreArchivo, anio, version, origen);
            return;
        }

        DatabaseManager.insertarPelicula(nombreArchivo, anio, version, extension, tamano, fechaModificacion, origen);
    }

    private static String obtenerFechaModificacion(File archivo) {
        long lastModified = archivo.lastModified();
        Date date = new Date(lastModified);
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        return formatter.format(date);
    }

    private static int obtenerAnio(File archivo) {
        String nombreArchivo = archivo.getName();
        Pattern pattern = Pattern.compile("\\((\\d{4})\\)");
        Matcher matcher = pattern.matcher(nombreArchivo);

        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                System.err.println("Error al convertir el anio a entero: " + e.getMessage());
            }
        }
        return 0;
    }

    private static String obtenerVersion(File archivo) {
        String nombre = archivo.getName().toLowerCase(Locale.ROOT);
        Pattern versionPattern = Pattern.compile("(3d|4k|uhd|hdr|1080p|720p)");
        Matcher matcher = versionPattern.matcher(nombre);
        if (matcher.find()) {
            return matcher.group(1).toUpperCase(Locale.ROOT);
        }
        return "";
    }

    private static String obtenerExtension(File archivo) {
        String nombreArchivo = archivo.getName();
        int puntoIndex = nombreArchivo.lastIndexOf('.');
        if (puntoIndex > 0) {
            return nombreArchivo.substring(puntoIndex + 1);
        }
        return "";
    }

    public static void generarInformeDuplicados(List<DuplicateEntry> duplicados) {
        Date fechaHoraEjecucion = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("ddMMyyyy_HHmm");
        String fechaHora = formatter.format(fechaHoraEjecucion);

        String nombreInforme = "InformeDuplicados_" + fechaHora + ".txt";
        File informe = new File(nombreInforme);

        try (FileWriter writer = new FileWriter(informe)) {
            writer.write("Fecha y hora de ejecucion: " + fechaHoraEjecucion + "\n\n");
            writer.write("Archivos duplicados:\n");
            for (DuplicateEntry duplicado : duplicados) {
                writer.write("- " + duplicado.titulo() + " (" + duplicado.anio() + ") [" + duplicado.version() + "] "
                        + duplicado.ruta() + " -> " + duplicado.motivo() + "\n");
            }
            System.out.println("Informe de duplicados generado: " + nombreInforme);
        } catch (IOException e) {
            System.out.println("Error al generar el informe de duplicados: " + e.getMessage());
        }
    }

    private static DriveOption solicitarUnidadParaEscaneo() {
        List<DriveOption> unidades = listarUnidades();
        if (unidades.isEmpty()) {
            System.out.println("No se encontraron unidades para escanear.");
            return null;
        }

        System.out.println("Seleccione la unidad a escanear:");
        for (int i = 0; i < unidades.size(); i++) {
            DriveOption opcion = unidades.get(i);
            String etiquetaPeliculas = opcion.tieneCarpetaPeliculas()
                    ? " - contiene carpeta '" + opcion.carpetaPeliculas().getName() + "'" : "";
            System.out.println((i + 1) + ") " + opcion.raiz().getAbsolutePath() + etiquetaPeliculas);
        }
        System.out.println("0) Salir");

        while (true) {
            System.out.print("Opcion: ");
            String entrada = CONSOLE.nextLine();
            if ("0".equals(entrada) || "salir".equalsIgnoreCase(entrada) || "s".equalsIgnoreCase(entrada)) {
                return null;
            }
            try {
                int indice = Integer.parseInt(entrada) - 1;
                if (indice >= 0 && indice < unidades.size()) {
                    return unidades.get(indice);
                }
            } catch (NumberFormatException ignored) {
            }
            System.out.println("Opcion no valida, intente de nuevo.");
        }
    }

    private static List<DriveOption> listarUnidades() {
        List<DriveOption> unidades = new ArrayList<>();
        File[] roots = File.listRoots();
        if (roots != null) {
            for (File root : roots) {
                if (root.exists() && root.canRead()) {
                    File carpetaPeliculas = encontrarCarpetaPeliculas(root);
                    boolean tienePeliculas = carpetaPeliculas != null;
                    unidades.add(new DriveOption(root, carpetaPeliculas, tienePeliculas));
                }
            }
        }
        return unidades;
    }

    private static File encontrarCarpetaPeliculas(File root) {
        String[] nombresCarpeta = { "peliculas", "peliculas2" };
        File[] directorios = root.listFiles(File::isDirectory);
        if (directorios == null) {
            return null;
        }
        for (File directorio : directorios) {
            for (String nombreCarpeta : nombresCarpeta) {
                if (directorio.getName().equalsIgnoreCase(nombreCarpeta)) {
                    return directorio;
                }
            }
        }
        return null;
    }

    private static void registrarDuplicado(String titulo, int anio, String version, String ruta) {
        archivosDuplicados.add(new DuplicateEntry(titulo, anio, version, ruta, "Titulo ya registrado"));
    }

    private static String solicitarNombreBaseDatos(String nombrePorDefecto) {
        System.out.println("Base de datos por defecto: " + nombrePorDefecto);
        System.out.print("Pulse Enter para usarla o escriba otro nombre: ");
        String entrada = CONSOLE.nextLine();
        if (entrada == null || entrada.trim().isEmpty()) {
            return nombrePorDefecto;
        }
        return entrada.trim();
    }

    private record DriveOption(File raiz, File carpetaPeliculas, boolean tieneCarpetaPeliculas) {
        File rutaParaEscanear() {
            return tieneCarpetaPeliculas ? carpetaPeliculas : raiz;
        }
    }

    private record DuplicateEntry(String titulo, int anio, String version, String ruta, String motivo) {
    }
}
