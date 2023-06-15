import click
import mysql.connector
import psycopg2
from colorama import Fore, Style, init
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter,merge_completers
from prettytable import PrettyTable
import time

from os import system

tipo_db_completer = WordCompleter(['mysql', 'postgresql'])

# Ruta del archivo de historial dentro del directorio del proyecto
history_file = "./history/.sql_history"

# Lista para almacenar el historial de comandos
command_history = []

init(autoreset=True)  # Inicializa Colorama para Windows

@click.command()
@click.option('--dbtype', prompt=f'{Fore.MAGENTA}Seleccione el tipo de base de datos:{Style.RESET_ALL} ',
              type=click.Choice(['mysql', 'postgresql'], case_sensitive=False),
              help='Tipo de base de datos a la que quieres conectarte',
            )
@click.option('--host', prompt=f'{Fore.MAGENTA}Host{Style.RESET_ALL}', help='Dirección IP o nombre de host de la base de datos')
@click.option('--port', prompt=f'{Fore.MAGENTA}Puerto{Style.RESET_ALL}', help='Puerto de la base de datos')
@click.option('--username', prompt=f'{Fore.MAGENTA}Nombre de usuario{Style.RESET_ALL}', help='Nombre de usuario de la base de datos')
@click.option('--password', prompt=f'{Fore.MAGENTA}Contraseña{Style.RESET_ALL}', hide_input=True, help='Contraseña de la base de datos')
@click.option('--database', prompt=f'{Fore.MAGENTA}Nombre de la base de datos{Style.RESET_ALL}', help='Nombre de la base de datos a la que te quieres conectar')

def cli(dbtype, host, port, username, password, database):
    if dbtype == 'mysql':
        # Conexión a MySQL
        
        try:
            conn = mysql.connector.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database
            )
            
            if conn.is_connected():
                cursor = conn.cursor()
                click.echo(f"{Fore.BLUE}Conexión exitosa a la base de datos: {Fore.YELLOW} {database}")
            else:
                click.echo(f"{Fore.RED}Error al conectar a la base de datos, por favor verifica el nombre!!!")
                return 
        except Exception as e:
            click.echo(f"{Fore.RED}Error al conectar a la base de datos: {e} ")
            return

            
        
        time.sleep(2)
        progress_over_iterable_with_colors()
                
        # Obtener la lista de tablas de la base de datos
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        mysql_completer = WordCompleter(['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER','INTO','VALUES','()',"=",'TABLE','(',')'] + tables, ignore_case=True)
        completer = merge_completers([WordCompleter(['exit']), mysql_completer])

        # Ejecución de comandos SQL
        while True:
            try:
                query = prompt('🧟 Mysql 🐜 ', completer=completer)
                
                match query:
                    case "exit":
                        break
                    case "clear" | "cls" | "CLEAR" | "CLS":
                        system("cls")
                        continue
                
                  # Actualizar el autocompletador con los campos de la tabla seleccionada
                if query.startswith('SELECT') and 'FROM' in query:
                    table_name = query.split('FROM')[1].strip().split()[0]
                    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
                    fields = [field[0] for field in cursor.fetchall()]
                    completer = WordCompleter(['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER','*','INTO','VALUES','()',"=",'TABLE','(',')'] + tables + fields, ignore_case=True)
                    
                
                cursor.execute(query)

                try:
                    results = cursor.fetchall()
                    if not results:
                        results = False
                except psycopg2.ProgrammingError:
                    results = []

                headers = [i[0] for i in cursor.description]
                if results:
                    table = PrettyTable(headers)
                    table.align = 'l'
                    for row in sorted(results, key=lambda x: x[0]):
                        table.add_row(row)
                    click.echo(Fore.GREEN + str(table))
                    command_history.append(query)
                else:
                    click.echo("No se encontraron resultados para la consulta MySQL!")

                conn.commit()

            except psycopg2.errors.SyntaxError:
                click.echo(Fore.RED + "Error de sintaxis en la consulta MySQL!!")
            except Exception as e:
                if str(e) == "No result set to fetch from.":
                     conn.commit()
                     command_history.append(query)
                else:
                    click.echo(Fore.RED + f"Error: {e}")


          
        #guardar el historial
        save_history(history_file)
        # Cierre de la conexión
        cursor.close()
        conn.close()
    elif dbtype == 'postgresql':
        # Conexión a PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        comandos_postgresql = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE',
            'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'DATABASE',
            'TRUNCATE', 'GRANT', 'REVOKE', 'VIEW', 'SEQUENCE', 'FUNCTION',
            'PROCEDURE', 'TRIGGER', 'DOMAIN', 'TYPE', 'CAST', 'EXPLAIN',
            'ANALYZE', 'CLUSTER', 'COPY', 'DISTINCT', 'GROUP BY', 'HAVING',
            'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN',
            'ORDER BY', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT',
        ]        
        
        completer_pg = WordCompleter(comandos_postgresql, ignore_case=True)

        # Ejecución de comandos SQL
        while True:
            query = prompt(f'{Fore.GREEN}postgresql> ', completer=completer_pg)
            if query == 'exit':
                break
            cursor.execute(query)
            results = cursor.fetchall()
            headers = [i[0] for i in cursor.description]
            if results:
                table = PrettyTable(headers)
                table.align = 'l'
                for row in sorted(results, key=lambda x: x[0]):
                    table.add_row(row)
                click.echo(Fore.GREEN + str(table))
            else:
                click.echo("No se encontraron resultados para la consulta SQL.")

        # Cierre de la conexión
        cursor.close()
        conn.close()
    else:
        click.echo('Tipo de base de datos no válido')



# Función para guardar el historial
def save_history(file_path):
    with open(file_path, 'w') as f:
        f.write('\n'.join(command_history))


    
def progress_over_iterable_with_colors():
    """
    Demonstrates how a progress bar can be tied to processing of
    an iterable - this time with colorful output.
    """

    # Could be a list, tuple and a whole bunch of other containers
    iterable = range(256)

    fill_char = click.style("#", fg="green")
    empty_char = click.style("-", fg="white", dim=True)
    label_text = f"{Fore.YELLOW} Conectando a la base de datos" 

    with click.progressbar(
            iterable=iterable,
            label=label_text,
            fill_char=fill_char,
            empty_char=empty_char
        ) as items:
        for item in items:
            # Do some processing
            time.sleep(0.010) # This is really hard work
            
if __name__ == '__main__':
    cli()