from pygments.lexers import JsonLexer


def setup(app):
    app.add_lexer("json", JsonLexer)
