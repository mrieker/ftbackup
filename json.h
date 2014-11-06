
#ifndef _JSON_H
#define _JSON_H

#include <stdio.h>
#include <string>

enum JSonType {
    JSONTYPE_BINARY,
    JSONTYPE_QUEUE,
    JSONTYPE_STACK,
    JSONTYPE_STRUC,
    JSONTYPE_STRING,
    JSONTYPE_UINTEGER
};

struct JSonBadChar {
};

struct JSonDelInList {
};

struct JSonInvalOp {
};

struct JSon {
    static JSon *ctor (FILE *file);

    JSon ();
    virtual ~JSon ();
    virtual JSon *find (char const *name);
    virtual JSon *poptop ();
    virtual JSonType type () =0;
    virtual std::string *getstring ();
    virtual unsigned long getbinsize ();
    virtual unsigned long getcount ();
    virtual unsigned long long getuinteger ();
    virtual void append (JSon *value);
    virtual void append (std::string *name, JSon *value);
    virtual void *getbindata ();

private:
    JSon *next;         // construct: set to self; destruct: check for eq self
    std::string *name;  // construct: set to NULL; destruct: deleted if non-NULL

    friend class JSonList;
    friend class JSonQueue;
    friend class JSonStack;
    friend class JSonStruc;
};

struct JSonBinary : JSon {
    JSonBinary (unsigned long s, void *d);
    virtual ~JSonBinary ();
    virtual JSonType type ();
    virtual unsigned long getbinsize ();
    virtual void *getbindata ();

private:
    unsigned long size;
    void *data;         // construct: set to given; destruct: freed if non-NULL
};

struct JSonList : JSon {
    JSonList ();
    virtual ~JSonList ();
    virtual JSon *poptop ();
    virtual JSonType type () =0;
    virtual unsigned long getcount ();

protected:
    JSon *elems;        // construct: set to NULL; destruct: all elements deleted
    unsigned long count;
};

struct JSonQueue : JSonList {
    JSonQueue ();
    virtual JSonType type ();
    virtual void append (JSon *value);

private:
    JSon **last;        // construct: set to &elems; destruct: ignored
};

struct JSonStack : JSonList {
    virtual JSonType type ();
    virtual void append (JSon *value);
};

struct JSonStruc : JSonList {
    virtual JSonType type ();
    virtual JSon *find (char const *name);
    virtual void append (std::string *name, JSon *value);
};

struct JSonString : JSon {
    JSonString (std::string *val);
    virtual ~JSonString ();
    virtual JSonType type ();
    virtual std::string *getstring ();

private:
    std::string *value; // construct: set to given; destruct: deleted
};

struct JSonUInteger : JSon {
    JSonUInteger (unsigned long long val);
    virtual JSonType type ();
    virtual unsigned long long getuinteger ();

private:
    unsigned long long value;
};

#endif
