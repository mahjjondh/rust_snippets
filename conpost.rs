use postgres::{error::Error, Client, NoTls};
use std::time::SystemTime;
#[derive(Debug)]
struct NomedaStruct {
    // use std::time::SystemTime para tratar dados do tipo datetime
}

fn print_db() -> Result<(), Error>{
    let mut conn = Client::connect("postgres://user:passsword@host:port/db" ,NoTls).unwrap();
    for row in &conn.query("Query no banco, use CASE e WHEN para tratar colunas NULL ou que precisem ser tratadas",&[],)?
    {
        let var_do_tipo_struct = NomedaStruct{
            nomecoluna: row.get(0),
            
        };
        
        println!("{}, {}", NomedaStruct.nomecoluna);
    }
Ok(())
}


fn main() -> Result<(), Error> {
    print_db()?;
    Ok(())
}
