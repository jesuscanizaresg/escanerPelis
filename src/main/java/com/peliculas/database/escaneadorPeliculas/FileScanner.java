package com.peliculas.database.escaneadorPeliculas;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class FileScanner {

	private static final Set<String> rutasProcesadas = new HashSet<>();

	public static void scanDirectory(String rutaDirectorio) {
		File directorio = new File(rutaDirectorio);
		if (!directorio.exists() || !directorio.isDirectory()) {
			System.err.println("El directorio no existe o no es valido: " + rutaDirectorio);
			return;
		}
		rutasProcesadas.clear();
		scanDirectorioRecursivo(directorio);
	}

	private static void scanDirectorioRecursivo(File directorio) {
		File[] archivos = directorio.listFiles();
		if (archivos != null) {
			for (File archivo : archivos) {
				if (archivo.isDirectory() && !archivo.getName().contains("SYNOINDEX MEDIA INFO")) {
					scanDirectorioRecursivo(archivo);
				} else if (archivo.isFile() && !archivo.getName().contains("SYNOINDEX MEDIA INFO")) {
					procesarPelicula(archivo);
				}
			}
		}
	}

	private static void procesarPelicula(File archivo) {
		String ruta = archivo.getAbsolutePath();
		if (rutasProcesadas.contains(ruta)) {
			return;
		}
		rutasProcesadas.add(ruta);
		EscaneadorPeliculas.procesarPelicula(archivo);
	}
}
