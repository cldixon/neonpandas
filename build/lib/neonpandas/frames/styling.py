import random
from collections import Counter 
from neonpandas.frames.nodeframe import NodeFrame
from neonpandas.frames.edgeframe import EdgeFrame

## Stylings for Node & Edge Frame classes. Provide
## visual aspects for exposing neonpandas conventions.
## more info:  https://pandas.pydata.org/pandas-docs/stable/user_guide/style.html

# Neo4j Bolt Browser color pallette 
bolt_colors = [
    '#F6C768',  # golden-yellow
    '#5C8DD4',  # blue
    '#659281',  # dark-green
    '#E4B8C9',  # pink 
    '#CD7694',  # magenta,
    '#C990C0',  # light purple
    '#9BCB98',  # light-green
    '#D7C9B1',  # brown
]

def get_bolt_colors(shuffle:bool=False):
    if shuffle is True:
        return random.sample(bolt_colors, len(bolt_colors))
    else:
        return bolt_colors


##############################################################################
#### CSS Formats for specific columns in Node & Edge Frame styled outputs ####
##############################################################################

# css formatting for node label cells ----
# border-radius is currently not being used; notice the comment out
# this created an effect similar to node visualization in Bolt browser
node_label_css = '''
                background-color: {lbl_color};
                /* border-radius: 50px */;
                '''

# css format for relationship type column ----
rel_type_css = '''
                background-color: {rel_color};
                border-style: solid;
                border-color: black;
                border-width: 1px;
                '''

edgeframe_node_css = '''
                    background-color: {lbl_color};
                    '''


def style_nodeframe(nf:NodeFrame, num_rows:int=10):
    """Main function for styling NodeFrame when printed."""
    return nf.head(num_rows).style.apply(
        style_node_labels, subset=['labels']
        ).set_caption(
            'NodeFrame'
        )

def style_edgeframe(ef:EdgeFrame, num_rows:int=10):
    """Main function for styling EdgeFrame when printed."""
    return ef.head(num_rows).style.apply(
        style_rel_types, subset=ef.rel_col
    ).set_caption(
        'EdgeFrame'
    )

def get_first_lbl(x:set, sorted_lbls):
    lbl_orders = {lbl: sorted_lbls[lbl] for lbl in x}
    return min(lbl_orders, key=lbl_orders.get)

def get_label_colors(data):
    print('!!! this is what the data to `get_label_colors()` looks like !!!!')
    print(data)
    print('----------------\n')
    lbl_counts = Counter([i for sublist in data for i in sublist])
    sorted_lbls = [k for k, v in sorted(lbl_counts.items(), key=lambda item: item[1])]
    sorted_lbls = {sorted_lbls[i]: i for i in range(len(sorted_lbls))}
    _colors = get_bolt_colors()
    lbl_color_map = {lbl: _colors[i % len(_colors)] for i,lbl in enumerate(sorted_lbls.keys())}
    lbl_colors = [lbl_color_map[get_first_lbl(x, sorted_lbls)] for x in data]
    return lbl_colors

def style_node_labels(data):
    lbl_colors = get_label_colors(data)
    return [node_label_css.format(lbl_color=lbl_color) for lbl_color in lbl_colors]

def style_rel_types(data, color:str='#A4AAB5'):
    return [rel_type_css.format(rel_color=color) for x in data]

def color_edgeframe_nodes(data):
    lbl_colors = get_label_colors([n.labels for n in data])
    return [edgeframe_node_css.format(lbl_color=lbl_color) for lbl_color in lbl_colors]
