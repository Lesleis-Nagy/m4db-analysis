import typer
import psycopg2
import os
import shutil

import pandas as pd

from rich.table import Column
from rich.progress import Progress, BarColumn, TextColumn

from m4db_analysis.utilities import uid_to_dir

app = typer.Typer()


def get_models_by_user_project(db_user: str, project_name: str, conn):
    r"""
    Retrieve all the models belonging to a user.
    :param db_user: the database use to which models belong.
    :param project_name: the name of the project to which models belong.
    :param conn: connection to the database.
    :return: a list of models.
    """
    with conn.cursor() as cursor:
        cursor.execute(r"""
        select 
            model.unique_id, 
            db_user.user_name, 
            project.name, 
            material.name, 
            material.temperature, 
            geometry.name, 
            geometry.size, 
            model.mx_tot, 
            model.my_tot, 
            model.mz_tot, 
            model.vx_tot, 
            model.vy_tot, 
            model.vz_tot, 
            model.h_tot, 
            model.adm_tot, 
            model.e_typical, 
            model.e_anis, 
            model.e_ext, 
            model.e_demag, 
            model.e_exch1, 
            model.e_exch2, 
            model.e_exch3, 
            model.e_exch4, 
            model.e_tot, 
            model.volume
        from model
            inner join metadata on model.mdata_id = metadata.id
            inner join db_user on metadata.db_user_id = db_user.id
            inner join project on metadata.project_id = project.id
            inner join running_status on model.running_status_id = running_status.id
            inner join geometry on model.geometry_id = geometry.id
            inner join model_material_association as mma on model.id = mma.model_id
            inner join material on material.id = mma.material_id
        where
            db_user.user_name like %(db_user)s and 
            project.name like %(project_name)s and 
            running_status.name like %(model_status)s
        """, {"db_user": db_user, "project_name": project_name, "model_status": "finished"})

        rows = cursor.fetchall()

    rows = [{"unique_id": row[0],
             "db_user": row[1],
             "project_name": row[2],
             "material": row[3],
             "temperature": row[4],
             "geometry": row[5],
             "size": row[6],
             "mx_tot": row[7],
             "my_tot": row[8],
             "mz_tot": row[9],
             "vx_tot": row[10],
             "vy_tot": row[11],
             "vz_tot": row[12],
             "h_tot": row[13],
             "adm_tot": row[14],
             "e_typical": row[15],
             "e_anis": row[16],
             "e_ext": row[17],
             "e_demag": row[18],
             "e_exch1": row[19],
             "e_exch2": row[20],
             "e_exch3": row[21],
             "e_exch4": row[22],
             "e_tot": row[23],
             "volume": row[24]} for row in rows]

    return rows


@app.command()
def retrieve_models(db_user: str, project_name: str, destination_dir: str, source_dir: str = "/exports/geos.ed.ac.uk/micro_magnetism/MMDatabase/fs_m4db_finetemp/model"):
    r"""
    Retrieve m4db models as a directory structure.
    :param db_user: the database use to which models belong.
    :param project_name: the name of the project to which models belong.
    :param destination_dir: the name of the destination directory.
    :return: None.
    """
    with psycopg2.connect("dbname=m4db_finetemp host=totaig") as conn:
        rows = get_models_by_user_project(db_user, project_name, conn)
        print(f"I retrieved {len(rows)} objects")

        text_column = TextColumn("{task.description}", table_column=Column(ratio=1))
        bar_column = BarColumn(bar_width=None, table_column=Column(ratio=2))
        progress = Progress(text_column, bar_column, expand=True)

        with progress:
            for row in progress.track(rows):

                output_dir = os.path.join(
                    destination_dir,
                    row["db_user"],
                    row["project_name"],
                    row["material"],
                    row["geometry"],
                    str(row["size"]),
                    str(row["temperature"])
                )

                output_zip = os.path.join(
                    output_dir, f"{row['unique_id']}.zip"
                )

                source_zip = os.path.join(
                    source_dir,
                    uid_to_dir(row["unique_id"]),
                    "data.zip"
                )

                os.makedirs(output_dir, exist_ok=True)
                shutil.copy(source_zip, output_zip)


@app.command()
def model_stats(db_user: str, project_name: str, output_csv: str):
    r"""
    Retrieve all the stats for m4db models and save them as a csv file.
    :param db_user: the database use to which models belong.
    :param project_name: the name of the project to which models belong.
    :param output_csv: the name of the output csv file.
    :return: None.
    """
    with psycopg2.connect("dbname=m4db_finetemp host=totaig") as conn:
        rows = get_models_by_user_project(db_user, project_name, conn)
        print(f"I retrieved {len(rows)} objects")

        df_dict = {"unique_id": [],
                   "db_user": [],
                   "project_name": [],
                   "material": [],
                   "temperature": [],
                   "geometry": [],
                   "size": [],
                   "mx_tot": [],
                   "my_tot": [],
                   "mz_tot": [],
                   "vx_tot": [],
                   "vy_tot": [],
                   "vz_tot": [],
                   "h_tot": [],
                   "adm_tot": [],
                   "e_typical": [],
                   "e_anis": [],
                   "e_ext": [],
                   "e_demag": [],
                   "e_exch1": [],
                   "e_exch2": [],
                   "e_exch3": [],
                   "e_exch4": [],
                   "e_tot": [],
                   "volume": []}

        for row in rows:
            df_dict["unique_id"].append(row["unique_id"])
            df_dict["db_user"].append(row["db_user"])
            df_dict["project_name"].append(row["project_name"])
            df_dict["material"].append(row["material"])
            df_dict["temperature"].append(row["temperature"])
            df_dict["geometry"].append(row["geometry"])
            df_dict["size"].append(row["size"])
            df_dict["mx_tot"].append(row["mx_tot"])
            df_dict["my_tot"].append(row["my_tot"])
            df_dict["mz_tot"].append(row["mz_tot"])
            df_dict["vx_tot"].append(row["vx_tot"])
            df_dict["vy_tot"].append(row["vy_tot"])
            df_dict["vz_tot"].append(row["vz_tot"])
            df_dict["h_tot"].append(row["h_tot"])
            df_dict["adm_tot"].append(row["adm_tot"])
            df_dict["e_typical"].append(row["e_typical"])
            df_dict["e_anis"].append(row["e_anis"])
            df_dict["e_ext"].append(row["e_ext"])
            df_dict["e_demag"].append(row["e_demag"])
            df_dict["e_exch1"].append(row["e_exch1"])
            df_dict["e_exch2"].append(row["e_exch2"])
            df_dict["e_exch3"].append(row["e_exch3"])
            df_dict["e_exch4"].append(row["e_exch4"])
            df_dict["e_tot"].append(row["e_tot"])
            df_dict["volume"].append(row["volume"])

        df = pd.DataFrame.from_dict(df_dict)
        df.to_csv(output_csv)


@app.command()
def count_models():
    r"""
    Count all the models in the database.
    :return: None
    """
    with psycopg2.connect("dbname=m4db_finetemp host=totaig") as conn:
        with conn.cursor() as cursor:
            cursor.execute(r"select count(1) from model")
            rows = cursor.fetchall()
            no_of_models = rows[0][0]
            print(f"There are {no_of_models} models.")


@app.command()
def count_nebs():
    r"""
    Count all the NEBs in the database.
    :return: None
    """
    with psycopg2.connect("dbname=m4db_finetemp host=totaig") as conn:
        with conn.cursor() as cursor:
            cursor.execute(r"select count(1) from neb")
            rows = cursor.fetchall()
            no_of_nebs = rows[0][0]
            print(f"There are {no_of_nebs} nebs.")


def main():
    app()
