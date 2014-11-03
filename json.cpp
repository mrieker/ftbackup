
#include "json.h"

#include <stdlib.h>
#include <string.h>

/**
 * @brief Read next value from given file.
 * @param file = file to read value from
 * @returns NULL: eof
 *          else: value read
 */
JSon *JSon::ctor (FILE *file)
{
    int c = fgetc (file);
    switch (c) {

        // binary data
        case '#': {
            unsigned long size = 0;
            while ((c = fgetc (file)) != EOF) {
                if (c == ':') break;
                if ((c < '0') || (c > '9')) throw new JSonBadChar ();
                size = size * 10 + c - '0';
            }
            void *data = NULL;
            if (size > 0) {
                data = malloc (size);
                if (data == NULL) return NULL;
                if (fread (data, size, 1, file) <= 0) {
                    free (data);
                    return NULL;
                }
            }
            return new JSonBinary (size, data);
        }

        // FIFO array (queue)
        case '[': {
            JSonQueue *queue = new JSonQueue ();
            while ((c = fgetc (file)) != EOF) {
                if (c == ']') break;
                if (c == ',') continue;
                ungetc (c, file);
                queue->append (JSon::ctor (file));
            }
            return queue;
        }

        // LIFO array (stack)
        case '<': {
            JSonStack *stack = new JSonStack ();
            while ((c = fgetc (file)) != EOF) {
                if (c == '>') break;
                if (c == ',') continue;
                ungetc (c, file);
                stack->append (JSon::ctor (file));
            }
            return stack;
        }

        // structure (named elements)
        case '{': {
            JSonStruc *struc = new JSonStruc ();
            while ((c = fgetc (file)) != EOF) {
                if (c == '}') break;
                if (c == ',') continue;
                std::string *name = new std::string ();
                do name->push_back (c);
                while (((c = fgetc (file)) != EOF) && (c != ':'));
                struc->append (name, JSon::ctor (file));
            }
            return struc;
        }

        // quoted string
        case '"': {
            std::string *val = new std::string ();
            while ((c = fgetc (file)) != EOF) {
                if (c == '"') break;
                if (c == '\\') {
                    c = fgetc (file);
                    if (c == EOF) break;
                    if (c == 'n') c = '\n';
                }
                val->push_back (c);
            }
            return new JSonString (val);
        }

        // unsigned integer
        case '0' ... '9': {
            unsigned long long val = c - '0';
            while ((c = fgetc (file)) != EOF) {
                if ((c < '0') || (c > '9')) break;
                val = (val * 10) + c - '0';
            }
            ungetc (c, file);
            return new JSonUInteger (val);
        }

        // end of file
        case EOF: {
            return NULL;
        }

        // who knows what?
        default: {
            ungetc (c, file);
            throw new JSonBadChar ();
        }
    }
}

// All values

JSon::JSon ()
{
    next = this;  // not part of a list
    name = NULL;  // not part of a struc
}

JSon::~JSon ()
{
    if (next != this) throw new JSonDelInList ();
    if (name != NULL) delete name;
}

JSon *JSon::find (char const *name)
{
     throw new JSonInvalOp ();
}

JSon *JSon::poptop ()
{
     throw new JSonInvalOp ();
}

std::string *JSon::getstring ()
{
    throw new JSonInvalOp ();
}

unsigned long JSon::getbinsize ()
{
    throw new JSonInvalOp ();
}

unsigned long JSon::getcount ()
{
    throw new JSonInvalOp ();
}

unsigned long long JSon::getuinteger ()
{
    throw new JSonInvalOp ();
}

void JSon::append (JSon *value)
{
    throw new JSonInvalOp ();
}

void JSon::append (std::string *name, JSon *value)
{
    throw new JSonInvalOp ();
}

void *JSon::getbindata ()
{
    throw new JSonInvalOp ();
}

// Binary

JSonBinary::JSonBinary (unsigned long s, void *d)
{
    size = s;
    data = d;
}

JSonBinary::~JSonBinary ()
{
    if (data != NULL) free (data);
}

JSonType JSonBinary::type ()
{
    return JSONTYPE_BINARY;
}

unsigned long JSonBinary::getbinsize ()
{
    return size;
}

void *JSonBinary::getbindata ()
{
    return data;
}

// List - includes queue, stack, struc

JSonList::JSonList ()
{
    elems = NULL;
    count = 0;
}

JSonList::~JSonList ()
{
    JSon *elem;
    while ((elem = poptop ()) != NULL) {
        delete elem;
    }
}

JSon *JSonList::poptop ()
{
    JSon *elem = elems;
    if (elem != NULL) {
        elems = elem->next;
        elem->next = elem;
        -- count;
    }
    return elem;
}

unsigned long JSonList::getcount ()
{
    return count;
}

// - Queue

JSonQueue::JSonQueue ()
{
    last = &elems;
}

JSonType JSonQueue::type ()
{
    return JSONTYPE_QUEUE;
}

void JSonQueue::append (JSon *value)
{
    *last = value;
     last = &value->next;
    *last = NULL;
    count ++;
}

// - Stack

JSonType JSonStack::type ()
{
    return JSONTYPE_STACK;
}

void JSonStack::append (JSon *value)
{
    value->next = elems;
    elems = value;
    count ++;
}

// Structures

JSonType JSonStruc::type ()
{
    return JSONTYPE_STRUC;
}

JSon *JSonStruc::find (char const *name)
{
    JSon *value;
    for (value = elems; value != NULL; value = value->next) {
        if (strcmp (value->name->c_str (), name) == 0) break;
    }
    return value;
}

void JSonStruc::append (std::string *name, JSon *value)
{
    value->next = elems;
    value->name = name;
    elems = value;
    count ++;
}

// String

JSonString::JSonString (std::string *val)
{
    value = val;
}

JSonString::~JSonString ()
{
    delete value;
}

JSonType JSonString::type ()
{
    return JSONTYPE_STRING;
}

std::string *JSonString::getstring ()
{
    return value;
}

// Unsigned Integer

JSonUInteger::JSonUInteger (unsigned long long val)
{
    value = val;
}

JSonType JSonUInteger::type ()
{
    return JSONTYPE_UINTEGER;
}

unsigned long long JSonUInteger::getuinteger ()
{
    return value;
}
