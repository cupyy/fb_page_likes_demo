import sys
import numpy as np 
import pandas as pd
from FB.user import User
from FB.page import Page
from FB.psqldb import PsqlDB



if __name__ == '__main__':
    try:
        token = sys.argv[1]
    except: 
        raise

    table_name = 'page_likes'
    min_page_likes_threshold = 5000
    db = PsqlDB()

    ########################################################################
    # recreate a new page_likes table
    ########################################################################

    db.drop_table(table_name)

    command = """CREATE TABLE {} ( 
        id SERIAL PRIMARY KEY, 
        user_name VARCHAR(255) NOT NULL, 
        page_title VARCHAR(255), 
        total_likes INTEGER, 
        about TEXT, 
        update_datetime timestamp default current_timestamp 
    )""".format(table_name)

    db.prepare_and_execute(command)
    ########################################################################
    # get all my available friends and pages they ever liked
    # then for each page retrieve info for that particular page (title, about, total likes...)
    # pages need to have more than 5000 likes to be considered
    ########################################################################

    me = User(access_token=token)
    if me.all_friends.empty:
        sys.exit('no firends to process')

    rows = []
    for id in me.all_friends.id:
        friend = User(access_token=token, id=id)
        pages = friend.likes_pages

        if pages.empty:
            continue

        pages = pages.set_index('id')
        for page_id in pages.index:
            page  = Page(access_token=token, id=page_id)
            if ( not page.stats.empty and page.stats['likes'] >= min_page_likes_threshold ):
                print ('friend id: {} page id:{}'.format(id, page_id) )

                rows.append({
                    'user_name': friend.content['name'],
                    'page_title': page.content['name'],
                    'about': page.stats['about'],
                    'total_likes': page.stats['likes']
                })
                

    ########################################################################
    # Insert those page records into postgresql db
    ########################################################################
    for r in rows:
        db.insert(table_name=table_name, data=r)


    ########################################################################
    # Retrieve top 3 pages with most total like count for each user by postgresql Windows function 
    # and print out result with pandas dataframe format
    ########################################################################
    command = """
        select *
        from (
            select 
                *,
                rank() OVER (
                    PARTITION BY {}
                    ORDER BY {}
                    DESC
                )
            from {} ) sub_query
        where rank <= 3    
    """.format('user_name', 'total_likes', table_name)

    rs = db.query(command)
    df = pd.DataFrame(rs)                
    print(df)


