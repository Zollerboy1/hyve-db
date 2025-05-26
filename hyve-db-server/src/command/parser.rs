use std::fmt::{self, Display};

use chumsky::{
    IterParser as _, Parser,
    error::Rich,
    extra,
    input::{BorrowInput, Input as _},
    prelude::{any, choice, group, just, recursive},
    select_ref,
    span::SimpleSpan,
    text::{self, ascii},
};

use super::{
    BinOp, Command, Expr, InsertCommand, ReadCommand, ResultColumn, SelectCommand, Value,
    WriteCommand,
};
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Null,
    Where,
    Is,
    Not,
    And,
    Or,
}

impl Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select => write!(f, "SELECT"),
            Self::From => write!(f, "FROM"),
            Self::Insert => write!(f, "INSERT"),
            Self::Into => write!(f, "INTO"),
            Self::Values => write!(f, "VALUES"),
            Self::True => write!(f, "TRUE"),
            Self::False => write!(f, "FALSE"),
            Self::Null => write!(f, "NULL"),
            Self::Where => write!(f, "WHERE"),
            Self::Is => write!(f, "IS"),
            Self::Not => write!(f, "NOT"),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Symbol {
    LeftParen,
    RightParen,
    Asterisk,
    Equal,
    Semicolon,
    Comma,
}

impl Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeftParen => write!(f, "("),
            Self::RightParen => write!(f, ")"),
            Self::Asterisk => write!(f, "*"),
            Self::Equal => write!(f, "="),
            Self::Semicolon => write!(f, ";"),
            Self::Comma => write!(f, ","),
        }
    }
}

impl Symbol {
    fn parser<'src>()
    -> impl Parser<'src, &'src str, Self, extra::Err<Rich<'src, char, SimpleSpan>>> + Clone {
        choice((
            just('(').to(Self::LeftParen),
            just(')').to(Self::RightParen),
            just('*').to(Self::Asterisk),
            just('=').to(Self::Equal),
            just(';').to(Self::Semicolon),
            just(',').to(Self::Comma),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Token<'src> {
    Keyword(Keyword),
    Identifier(&'src str),
    Float(f64),
    Integer(i64),
    String(&'src str),
    Symbol(Symbol),
}

impl Display for Token<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Keyword(keyword) => keyword.fmt(f),
            Self::Identifier(ident) => ident.fmt(f),
            Self::Float(float) => float.fmt(f),
            Self::Integer(int) => int.fmt(f),
            Self::String(string) => string.fmt(f),
            Self::Symbol(symbol) => symbol.fmt(f),
        }
    }
}

impl<'src> Token<'src> {
    fn parser()
    -> impl Parser<
        'src,
        &'src str,
        Vec<(Self, SimpleSpan)>,
        extra::Err<Rich<'src, char, SimpleSpan>>,
    > + Clone {
        let ident = ascii::ident().map(|s: &str| match s.to_lowercase().as_str() {
            "select" => Self::Keyword(Keyword::Select),
            "from" => Self::Keyword(Keyword::From),
            "insert" => Self::Keyword(Keyword::Insert),
            "into" => Self::Keyword(Keyword::Into),
            "values" => Self::Keyword(Keyword::Values),
            "true" => Self::Keyword(Keyword::True),
            "false" => Self::Keyword(Keyword::False),
            "null" => Self::Keyword(Keyword::Null),
            "where" => Self::Keyword(Keyword::Where),
            "is" => Self::Keyword(Keyword::Is),
            "not" => Self::Keyword(Keyword::Not),
            "and" => Self::Keyword(Keyword::And),
            "or" => Self::Keyword(Keyword::Or),
            _ => Self::Identifier(s),
        });

        let quoted = |c| {
            any()
                .and_is(just(c).not())
                .repeated()
                .to_slice()
                .delimited_by(just(c), just(c))
        };

        choice((
            ident,
            quoted('"').map(Self::Identifier),
            quoted('\'').map(Self::String),
            just('-')
                .or_not()
                .then(text::int(10))
                .then(just('.'))
                .then(text::int(10))
                .to_slice()
                .map(str::parse)
                .map(Result::unwrap)
                .map(Self::Float),
            just('-')
                .or_not()
                .then(text::int(10))
                .to_slice()
                .map(str::parse)
                .map(Result::unwrap)
                .map(Self::Integer),
            Symbol::parser().map(Self::Symbol),
        ))
        .map_with(|tok, e| (tok, e.span()))
        .padded()
        .repeated()
        .collect()
    }
}

impl Value {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        select_ref! {
            Token::Keyword(Keyword::True) => Self::Integer(1),
            Token::Keyword(Keyword::False) => Self::Integer(0),
            Token::Keyword(Keyword::Null) => Self::Null,
            Token::Float(f) => Self::Real(*f),
            Token::Integer(i) => Self::Integer(*i),
            Token::String(s) => Self::Text(s.to_string()),
        }
    }
}

impl ResultColumn {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        select_ref! {
            Token::Symbol(Symbol::Asterisk) => Self::Wildcard,
            Token::Identifier(name) => Self::Column(name.to_string()),
        }
    }
}

impl BinOp {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        select_ref! {
            Token::Symbol(Symbol::Equal) => Self::Eq,
            Token::Keyword(Keyword::And) => Self::And,
            Token::Keyword(Keyword::Or) => Self::Or,
        }
        .or(just(Token::Keyword(Keyword::Is))
            .ignore_then(just(Token::Keyword(Keyword::Not)).or_not())
            .map(|not| not.map_or(Self::Is, |_| Self::IsNot)))
    }
}

impl Expr {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        recursive(|expr| {
            let primary = choice((
                Value::parser().map(Self::Value),
                select_ref! {
                    Token::Identifier(name) => Self::Column(name.to_string()),
                },
                expr.delimited_by(
                    just(Token::Symbol(Symbol::LeftParen)),
                    just(Token::Symbol(Symbol::RightParen)),
                ),
            ));

            let binary = primary.clone().foldl(
                BinOp::parser().then(primary).repeated(),
                |lhs, (op, rhs)| Self::BinOp(op, Box::new(lhs), Box::new(rhs)),
            );

            binary
        })
    }
}

impl SelectCommand {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        let result_columns = ResultColumn::parser()
            .separated_by(just(Token::Symbol(Symbol::Comma)))
            .collect();

        let ident = chumsky::select_ref! {
            Token::Identifier(name) => name.to_string(),
        };

        group((
            just(Token::Keyword(Keyword::Select)).ignore_then(result_columns),
            just(Token::Keyword(Keyword::From)).ignore_then(ident),
            just(Token::Keyword(Keyword::Where))
                .ignore_then(Expr::parser())
                .or_not(),
        ))
        .map(|(result_columns, table, where_clause)| Self {
            table,
            result_columns,
            where_clause,
        })
    }
}

impl InsertCommand {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        let ident = select_ref! {
            Token::Identifier(name) => name.to_string(),
        };

        let columns = ident
            .separated_by(just(Token::Symbol(Symbol::Comma)))
            .collect()
            .delimited_by(
                just(Token::Symbol(Symbol::LeftParen)),
                just(Token::Symbol(Symbol::RightParen)),
            );

        let values = Value::parser()
            .separated_by(just(Token::Symbol(Symbol::Comma)))
            .collect()
            .delimited_by(
                just(Token::Symbol(Symbol::LeftParen)),
                just(Token::Symbol(Symbol::RightParen)),
            );

        just(Token::Keyword(Keyword::Insert))
            .then(just(Token::Keyword(Keyword::Into)))
            .ignore_then(group((
                ident,
                columns,
                just(Token::Keyword(Keyword::Values)).ignore_then(values),
            )))
            .boxed()
            .map(|(table, columns, values)| Self {
                table,
                id: None,
                columns,
                values,
            })
    }
}

impl ReadCommand {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        SelectCommand::parser().map(Self::Select)
    }
}

impl WriteCommand {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        InsertCommand::parser().map(Self::Insert)
    }
}

impl Command {
    fn parser<'lex, 'src, I>()
    -> impl Parser<'lex, I, Self, extra::Err<Rich<'lex, Token<'src>, SimpleSpan>>> + Clone
    where
        I: BorrowInput<'lex, Token = Token<'src>, Span = SimpleSpan>,
        'src: 'lex,
    {
        choice((
            ReadCommand::parser().map(Self::Read),
            WriteCommand::parser().map(Self::Write),
        ))
        .then_ignore(just(Token::Symbol(Symbol::Semicolon)))
    }

    pub fn parse(input: &str) -> Result<Self> {
        let tokens = match Token::parser().parse(input).into_output_errors() {
            (Some(tokens), _) => tokens,
            (None, errors) => {
                return Err(Error::Parser(
                    errors
                        .iter()
                        .map(ToString::to_string)
                        .intersperse("; ".to_string())
                        .collect(),
                ));
            }
        };

        let tokens = tokens
            .as_slice()
            .map((input.len()..input.len()).into(), |(t, s)| (t, s));

        match Self::parser().parse(tokens).into_output_errors() {
            (Some(command), _) => Ok(command),
            (None, errors) => {
                return Err(Error::Parser(
                    errors
                        .iter()
                        .map(ToString::to_string)
                        .intersperse("; ".to_string())
                        .collect(),
                ));
            }
        }
    }
}
