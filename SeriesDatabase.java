package series;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class SeriesDatabase {
	
	private static Connection conn=null;
	private Statement st=null;
	private ResultSet rs=null;
	private PreparedStatement pst=null;

	public SeriesDatabase() {
		try{
			Class.forName("com.mysql.cj.jdbc.Driver");
		}catch(ClassNotFoundException e){
			System.out.println("No se ha podido cargar la clase Driver");
		}
	}
//metodo auxiliar/////
private void mrClean() {
		try {
			if(st != null) st.close(); //cierra statement
			if(rs != null) rs.close(); //cierra resultset
			if(pst != null) pst.close(); //cierra statement
			if (conn != null && conn.getAutoCommit() == false) conn.setAutoCommit(true); //si el autocomit esta a false lo pone a true
		} catch (SQLException e) {
			System.out.printf("Error al realizar los cierres");
			e.printStackTrace();
		}
	}
//////////////////	
	public boolean openConnection() {
		
		boolean resul = true;
		
		if(conn==null) {
			String serverAddress = "localhost:3306";
			String db = "series";
			String user = "series_user";
			String pass = "series_pass";
			String url = "jdbc:mysql://" + serverAddress + "/" + db;
			try {
				conn = DriverManager.getConnection(url, user, pass);
				System.out.println("Conectado a la base de datos");
				System.out.println(url);
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("No se ha podido conectar con la base de datos");
				resul = false;
			}
		}
		
		return resul;
	}

	public boolean closeConnection() {
		
		boolean resul = true;
		try{
			mrClean();
			if(conn != null) conn.close();
			System.out.println("Desconectado de la base de datos");
		} 
		catch (SQLException e){
			e.printStackTrace();
			System.out.println("No se ha podido desconectar con la base de datos");
			resul = false;
			}
		return resul;
	}

	public boolean createTableCapitulo() {
		
		openConnection();
		
		boolean resul = true;
				
		try {
			String query = 	"CREATE TABLE capitulo ( "+
							"id_serie INT NOT NULL, "+
							"n_temporada INT NOT NULL, "+
							"n_orden INT NOT NULL, "+
							"fecha_estreno DATE, "+
							"titulo VARCHAR(100), "+
							"duracion INT, "+
							"PRIMARY KEY (id_serie, n_temporada, n_orden), "+ 
							"FOREIGN KEY (id_serie) REFERENCES serie(id_serie), "+
							"FOREIGN KEY (id_serie, n_temporada) REFERENCES temporada(id_serie, n_temporada) "+
							");";
			
			st = conn.createStatement();
			st.executeUpdate(query);
			System.out.println("Tabla creada correctamente");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("No se ha podido crear la tabla");
			resul = false;
		}
		finally {
			mrClean();
		}
		return resul;
	}

	public boolean createTableValora() {
		
		openConnection();
		
		boolean resul = true;
		
		try {
			String query = 	"CREATE TABLE valora ( "+
							"id_serie INT NOT NULL, "+
							"n_temporada INT NOT NULL, "+
							"n_orden INT NOT NULL, "+
							"id_usuario INT NOT NULL, "+
							"fecha DATE NOT NULL, "+
							"valor INT, "+
							"PRIMARY KEY (id_usuario, fecha, n_orden, id_serie, n_temporada), "+
							"FOREIGN KEY (id_usuario) REFERENCES usuario(id_usuario), "+
							"FOREIGN KEY (id_serie, n_temporada, n_orden) REFERENCES capitulo(id_serie, n_temporada, n_orden), "+
							"FOREIGN KEY (id_serie) REFERENCES serie(id_serie), "+
							"FOREIGN KEY (id_serie, n_temporada) REFERENCES temporada(id_serie, n_temporada) "+
							");";
			
			st = conn.createStatement();
			st.executeUpdate(query);
			System.out.println("Tabla creada correctamente");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("No se ha podido crear la tabla");
			resul = false;
		}
		finally {
			mrClean();
		}
		return resul;
	}

	public int loadCapitulos(String fileName) {
		
		openConnection();
		ArrayList<String> tcapitulo = new ArrayList<String>();
		String querry="INSERT INTO capitulo (id_serie, n_temporada, n_orden, fecha_estreno, titulo, duracion) VALUES(?,?,?,?,?,?);";
		try {
			BufferedReader br = Files.newBufferedReader(Paths.get(fileName), Charset.forName("ISO-8859-1"));
			String line;
			while((line = br.readLine()) != null){
			   tcapitulo.add(line);
		   }
		    PreparedStatement pst=conn.prepareStatement(querry);
			for(int i=1;i<tcapitulo.size();i++){
				String[] capitulo=tcapitulo.get(i).split(";");
				String[] date=capitulo[3].split("-");
				pst.setInt(1, Integer.parseInt(capitulo[0]));
				pst.setInt(2, Integer.parseInt(capitulo[1]));
				pst.setInt(3, Integer.parseInt(capitulo[2]));
				pst.setDate(4,new java.sql.Date(Integer.parseInt(date[0])-1900,Integer.parseInt(date[1]) -1,Integer.parseInt(date[2])));
				pst.setString(5, capitulo[4]);
				pst.setInt(6, Integer.parseInt(capitulo[5]));
				pst.executeUpdate();
			}
				} catch (SQLException e) {
					e.printStackTrace();
					System.out.println("No se ha podido crear la tabla");
				}catch (IOException e) {
					e.printStackTrace();
				}
				finally {
					mrClean();
				}
		return tcapitulo.size();
	}

	public int loadValoraciones(String fileName) {
		
		openConnection();
		ArrayList<String> tValoraciones = new ArrayList<String>();
		String querry="INSERT INTO valora (id_serie, n_temporada, n_orden, id_usuario, fecha, valor) VALUES(?,?,?,?,?,?);";
		try {
			BufferedReader br = Files.newBufferedReader(Paths.get(fileName), Charset.forName("ISO-8859-1"));
			String line;
			while((line = br.readLine()) != null){
			   tValoraciones.add(line);
		   }
		    PreparedStatement pst=conn.prepareStatement(querry);
			for(int i=1;i<tValoraciones.size();i++){
				String[] valoracion=tValoraciones.get(i).split(";");
				String[] date=valoracion[4].split("-");
				pst.setInt(1, Integer.parseInt(valoracion[0]));
				pst.setInt(2, Integer.parseInt(valoracion[1]));
				pst.setInt(3, Integer.parseInt(valoracion[2]));
				pst.setInt(4, Integer.parseInt(valoracion[3]));
				pst.setDate(5, new java.sql.Date(Integer.parseInt(date[0])-1900,Integer.parseInt(date[1]) -1,Integer.parseInt(date[2])));
				pst.setInt(6, Integer.parseInt(valoracion[5]));
				pst.executeUpdate();
			}
				} catch (SQLException e) {
					e.printStackTrace();
					System.out.println("No se ha podido crear la tabla");
				}catch (IOException e) {
					e.printStackTrace();
				}
				finally {
					mrClean();
				}
		return tValoraciones.size();
	}

	public String catalogo() {
		openConnection();
		String res="{";
		try{
			st=conn.createStatement();
			String querry="SELECT titulo, n_capitulos "
						+ "FROM temporada t "
						+ "INNER JOIN serie s ON t.ID_serie=s.ID_serie;";
			rs=st.executeQuery(querry);
			System.out.println("Query ejecutada");
			if(rs==null){
				return "{}";
			}
			rs.next();
			String serie=rs.getString("titulo");
			res=res+serie+":["+rs.getInt("n_capitulos")+",";
			boolean first=true;
			
			while(rs.next()){
				if(serie.equals(rs.getString("titulo"))) {
					if(first){
						res=res+rs.getInt("n_capitulos");
						first=false;
					}
					else {
					res=res+","+rs.getInt("n_capitulos");
					}
				}
				else{
					serie=rs.getString("titulo");
					res=res+"],"+serie+":["+rs.getInt("n_capitulos");
				}
			}
				
			}catch(SQLException e){
			System.out.println("Error query");	
			System.err.println(e.getMessage());
			return null;
			}
			finally{
				mrClean();
			}
		res=res+"]}";
		return res;
	}
	
	public String noHanComentado() {
		openConnection();
		String res="[";
		try{
			st=conn.createStatement();
			String querry="SELECT nombre, apellido1, apellido2 "
						+ "FROM usuario u "
						+ "LEFT JOIN comenta c ON u.id_usuario=c.id_usuario "
						+ "WHERE c.id_usuario IS NULL;";
			rs=st.executeQuery(querry);
			if(rs==null){
				return "[]";
			}
			boolean first=true;
			while(rs.next()){
				if(first){
				res=res+rs.getString("nombre")+" "+rs.getString("apellido1")+" "+rs.getString("apellido2");//algo asi
				}
				else{
				res=res+", "+rs.getString("nombre")+" "+rs.getString("apellido1")+" "+rs.getString("apellido2");	
				}
				first=false;
				
			}

			}catch(SQLException e){
			System.out.println("Error");	
			System.err.println(e.getMessage());
			return null;
			}
			finally{
				mrClean();
			}
		res=res+"]";
		return res;
	}

	public double mediaGenero(String genero) {
		openConnection();
		double resul=0;
		try {
			String querry="Select id_genero from genero WHERE descripcion=?;";
			pst=conn.prepareStatement(querry);
			pst.setString(1, genero);
			rs=pst.executeQuery();
			if(rs.next()) {
				querry="select AVG(valor)"
						+ "		from valora v"
						+ "		inner join capitulo c on v.n_orden=c.n_orden AND v.n_temporada=c.n_temporada AND v.id_serie=c.id_serie"
						+ "		inner join serie s on c.id_serie=s.id_serie"
						+ "		inner join pertenece p on s.id_serie=p.id_serie"
						+ " 	inner join genero g on p.id_genero=g.id_genero"
						+ "		WHERE g.descripcion=?;";
				pst=conn.prepareStatement(querry);
				pst.setString(1, genero);
				rs=pst.executeQuery();
				if(rs.next()) {
					resul=rs.getDouble(1);		
				}
			}
			else {
				resul=-1;
			}
		} catch (SQLException e) {
			System.out.println("Error en la querry");
			System.err.println(e.getMessage());
			resul=-2;
		} finally {
			mrClean();
		}

		return resul;
	}
	
	public double duracionMedia(String idioma) {
		openConnection();
		double res=0;
		try{
			String querry="select AVG(duracion)"
					+ "	from capitulo c"
					+ "	LEFT JOIN valora v on  v.n_orden=c.n_orden AND v.n_temporada=c.n_temporada AND v.id_serie=c.id_serie"
					+ " INNER JOIN serie s on c.id_serie=s.id_serie"
					+ " WHERE v.n_orden IS NULL AND v.n_temporada IS NULL AND v.id_serie IS NULL AND s.idioma = ?;";
			pst=conn.prepareStatement(querry);
			pst.setString(1, idioma);
			rs=pst.executeQuery();
			if(rs.next()) {
				res=rs.getDouble(1);
				}
			if(res==0) {
				res=-1;
			}
		} catch (SQLException e) {
			System.out.println("Error en la querry");
			System.err.println(e.getMessage());
			res=-2;
		} finally {
			mrClean();
		}
		return res;
	}

	public boolean setFoto(String filename) {
		openConnection();
		boolean res = false;
		File f=null;
		FileInputStream fis=null;
		try{
			st=conn.createStatement();
			String querry = "SELECT count(*) FROM usuario WHERE apellido1 = \"Cabeza\" AND fotografia IS NULL;";
			rs=st.executeQuery(querry);
			if(rs.next() && rs.getInt(1)==1) {
				f=new File(filename);
				fis=new FileInputStream(f);
				querry="UPDATE usuario SET fotografia = ? WHERE apellido1 = \"Cabeza\" AND fotografia IS NULL;";
				pst=conn.prepareStatement(querry);
				pst.setBinaryStream(1, fis, (int)f.length());
				pst.executeUpdate();
				res=true;
				System.out.println("Foto añadida correctamente");
			}
			else{
				System.out.println("Foto no añadida");
			}
		}catch(SQLException e){
			System.err.println("Error sql al añadir la foto");
			System.err.println(e.getMessage());
		}catch(FileNotFoundException e){
			System.err.println("Error en el archivo");
			System.err.println(e.getMessage());
		}catch(Exception e){
			System.err.println("Error en el archivo");
			System.err.println(e.getMessage());
		}finally{
			mrClean();
			if(fis!=null)
				try {
					fis.close();
				} catch (IOException e) {
					System.err.println("error al cerrar fis en la estructura");
					e.printStackTrace();
				}
		}
		return res;
	}
}