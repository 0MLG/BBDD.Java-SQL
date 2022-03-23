package series;

import java.util.ArrayList;
import java.util.Scanner;

public class Main {

	public static void main (String[] args) {

		Scanner sc = new Scanner(System.in);

		SeriesDatabase series = new SeriesDatabase();
		char menuOption = 'a';

		// Main menu loop
		do {
			System.out.println("Escoja una opci�n: ");
			System.out.println("  1) Crear las tablas \"capitulo\" y \"valora\".");
			System.out.println("  2) Cargar datos  de las tablas \"capitulo\" y \"valora\".");
			System.out.println("  3) Mostrar el cat�logo.");
			System.out.println("  4) Mostrar usuarios que no han comentado.");
			System.out.println("  5) Obtener la valoraci�n media de un g�nero.");
			System.out.println("  6) Obtener la duraci�n media de cap�tulos de un idioma y sin valoraciones.");
			System.out.println("  7) Poner foto de perfil.");
			System.out.println("  0) Salir de la aplicaci�n.");

			// Read user's option and check that it is a valid option
			menuOption = 'a';
			do {
				String line = sc.nextLine();
				if (line.length()==1) {
					menuOption = line.charAt(0);
				}
				if (menuOption<'0' || menuOption>'7') {
					System.out.println("Opci�n incorrecta.");
				}
			} while (menuOption<'0' || menuOption>'7');

			// Call a specific method depending on the option
			switch (menuOption) {
			case '1':
				System.out.println("Creando tabla \"capitulo\"...");
				series.createTableCapitulo();
				System.out.println("Creando tabla \"valora\"...");
				series.createTableValora();
				break;
			case '2':
				System.out.println("Cargando datos de la tabla \"capitulo\"...");
				int n = series.loadCapitulos("capitulos.csv");
				System.out.println("Se han cargado " + n + " entradas.");
				System.out.println("Cargando datos de la tabla \"valora\"...");
				n = series.loadValoraciones("valoraciones.csv");
				System.out.println("Se han cargado " + n + " entradas.");
				break;
			case '3':
				System.out.println("Cat�logo disponible:");
				System.out.println(series.catalogo());
				break;
			case '4':
				System.out.println("Usuarios que no han comentado:");
				System.out.println(series.noHanComentado());
				break;
			case '5':
				System.out.println("�De qu� gen�ro quieres consultar la valoraci�n media?");
				String genero = sc.nextLine();
				double valoracion = series.mediaGenero(genero);
				System.out.println("La valoraci�n media del g�nero " + genero + " es de " + valoracion + ".");
				break;
			case '6':
				System.out.println("�Qu� idioma quieres seleccionar?");
				String idioma = sc.nextLine();
				double duracion = series.duracionMedia(idioma);
				System.out.println("La duraci�n media es de " + duracion + ".");
				break;
			case '7':
				System.out.println("Cargando imagen...");
				series.setFoto("HomerSimpson.jpg");
				break;
			}

			if (menuOption!='0')
				System.out.println("�Qu� m�s desea hacer?");
			else
				System.out.println("�Hasta pronto!");
		} while (menuOption!='0');

		sc.close();
		
		series.closeConnection();
	}

}
